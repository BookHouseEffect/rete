import { Component } from './component';
import { Context } from '../core/context';
import { Recursion } from './recursion';
import { State } from './state';
import { Validator } from '../core/validator';
import { Data, NodeData, WorkerOutputs } from '../core/data';
import { EngineEvents, EventsTypes } from './events';
export { Component, Recursion };

interface EngineNode extends NodeData {
    busy: boolean;
    unlockPool: (() => void)[];
    outputData: WorkerOutputs;
}

interface DependencyGraph { 
    nodeId: number; 
    nodeDependsOnIds: number[]; 
    addedToOrder: boolean; 
}

export class Engine extends Context<EventsTypes> {

    args: unknown[] = [];
    data: Data | null = null;
    state = State.AVAILABLE;
    onAbort = () => { };

    private dependencies: { [key: string]: DependencyGraph } = {};
    private processingOrder: number[] = [];

    constructor(id: string) {
        super(id, new EngineEvents());
    }

    public clone() {
        const engine = new Engine(this.id);

        this.components.forEach(c => engine.register(c));

        return engine;
    }

    async throwError (message: string, data: unknown = null) {
        await this.abort();
        this.trigger('error', { message, data });
        this.processDone();

        return 'error';
    }

    private processStart() {
        if (this.state === State.AVAILABLE) {  
            this.state = State.PROCESSED;
            return true;
        }

        if (this.state === State.ABORT) {
            return false;
        }

        console.warn(`The process is busy and has not been restarted.
                Use abort() to force it to complete`);
        return false;
    }

    private processDone() {
        const success = this.state !== State.ABORT;

        this.state = State.AVAILABLE;
        
        if (!success) {
            this.onAbort();
            this.onAbort = () => { }
        }    

        return success;
    }

    public async abort() {
        return new Promise(ret => {
            if (this.state === State.PROCESSED) {
                this.state = State.ABORT;
                this.onAbort = ret;
            }
            else if (this.state === State.ABORT) {
                this.onAbort();
                this.onAbort = ret;
            }
            else
                ret();
        });
    }

    private async lock(node: EngineNode) {
        return new Promise(res => {
            node.unlockPool = node.unlockPool || [];
            if (node.busy && !node.outputData)
                node.unlockPool.push(res);
            else 
                res();
            
            node.busy = true;
        });    
    }

    unlock(node: EngineNode) {
        node.unlockPool.forEach(a => a());
        node.unlockPool = [];
        node.busy = false;
    }

    private async extractInputData(node: NodeData) {
        const obj: {[id: string]: any} = {};

        for (let key of Object.keys(node.inputs)) {
            const input = node.inputs[key];
            const conns = input.connections;
            const connData = await Promise.all(conns.map(async (c) => {
                const prevNode = (this.data as Data).nodes[c.node];

                const outputs = await this.processNode(prevNode as EngineNode);

                if (!outputs) 
                    this.abort();
                else
                    return outputs[c.output];
            }));

            obj[key] = connData;
        }

        return obj;
    }

    private async processWorker(node: NodeData) {
        const inputData = await this.extractInputData(node);
        const component = this.components.get(node.name) as Component;
        const outputData = {};

        try {
            await component.worker(node, inputData, outputData, ...this.args);
        } catch (e) {
            this.abort();
            this.trigger('warn', e);
        }

        return outputData;
    }

    private async processNode(node: EngineNode) {
        if (this.state === State.ABORT || !node)
            return null;
        
        await this.lock(node);

        if (!node.outputData) {
            node.outputData = await this.processWorker(node);
        }

        this.unlock(node);
        return node.outputData;
    }

    private async forwardProcess(node: NodeData) {
        if (this.state === State.ABORT)
            return null;

        const index = this.processingOrder.indexOf(node.id);
        const nextNodesIds = this.processingOrder.slice(index + 1);
       
        return await Promise.all(nextNodesIds.map(async (n) => {
            const nextNode = (this.data as Data).nodes[n];

            await this.processNode(nextNode as EngineNode);
        }));
    }

    copy(data: Data) {
        data = Object.assign({}, data);
        data.nodes = Object.assign({}, data.nodes);
        
        Object.keys(data.nodes).forEach(key => {
            data.nodes[key] = Object.assign({}, data.nodes[key])
        });
        return data;
    }

    async validate(data: Data) {
        const checking = Validator.validate(this.id, data);
        const recursion = new Recursion(data.nodes);

        if (!checking.success)
            return await this.throwError(checking.msg);  
        
        const recurrentNode = recursion.detect();

        if (recurrentNode)
            return await this.throwError('Recursion detected', recurrentNode);      
         
        return true;
    }

    private addNodeToProcessingOrder(x: DependencyGraph) {
        if (this.state === State.ABORT)
            return null;

        for (let n in x.nodeDependsOnIds) {
            const item = x.nodeDependsOnIds[+n];
            const dependency = this.dependencies[`${item}`];

            if (!dependency.addedToOrder && this.processingOrder.indexOf(item) === -1) {
                dependency.addedToOrder = true;
                this.addNodeToProcessingOrder(dependency);
            }
        }
        
        if (this.processingOrder.indexOf(x.nodeId) === -1) {
            x.addedToOrder = true;
            this.processingOrder.push(x.nodeId);
        }
    }

    private buildDependencyGraph() {
        if (this.state === State.ABORT)
            return null;
        
        this.dependencies = {};  
        const data = this.data as Data;

        Object.keys(data.nodes).forEach(key => {
            const node = data.nodes[key];

            const dependsOn = ([] as number[]).concat.apply([], Object.keys(node.inputs).map(inKey => {
                return node.inputs[inKey].connections.map(x => x.node);
            })).filter((value, index, array) => { 
                return array.indexOf(value) === index 
            });

            this.dependencies[key] = {
                nodeId: node.id,
                nodeDependsOnIds: dependsOn,
                addedToOrder: false
            };
        });
    }

    private buildProcessingOrder() {
        if (this.state === State.ABORT)
            return null;

        this.processingOrder = [];
        for (let i in this.dependencies) {
            if (!this.dependencies[i].addedToOrder && 
                this.processingOrder.indexOf(this.dependencies[i].nodeId) === -1) {
                this.dependencies[i].addedToOrder = true;
                this.addNodeToProcessingOrder(this.dependencies[i]);
            }
        }
    }

    private async processStartNode(id: string | number | null) {
        if (!id) return;

        let startNode = (this.data as Data).nodes[id];

        if (!startNode)
            return await this.throwError('Node with such id not found');   
        
        await this.processNode(startNode as EngineNode);
        await this.forwardProcess(startNode);
    }

    async process<T extends unknown[]>(data: Data, startId: number | string | null = null, ...args: T) {
        if (!this.processStart()) return;
        if (!this.validate(data)) return;    
        
        this.data = this.copy(data);
        this.args = args;

        this.buildDependencyGraph();
        this.buildProcessingOrder();
        console.debug(startId);
        
        const id = this.processingOrder.length ? this.processingOrder[0] : null;

        await this.processStartNode(id);

        return this.processDone()?'success':'aborted';
    }
}