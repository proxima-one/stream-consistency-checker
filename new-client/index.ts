
import {ProximaStreamClient as StreamClient, Offset, StreamEvent,  StreamRegistryClient, StreamStats} from "@proximaone/stream-client-js"



type StreamFetcherConfig = {
    endpoint: string;
    retryPolicy?: {
        waitInMs: number,
        retryCount: number,
    }
    options?: any;
 }

const DefaultRetryPolicy = {
    waitInMs: 5000,
    retryCount: 10
}

export class NewClient {
    streamOffsets: Record<string, Offset>;
    registry: StreamRegistryClient;
    client: StreamClient; 

    public constructor(config: StreamFetcherConfig) {
        this.registry = new StreamRegistryClient({endpoint: config.endpoint, retryPolicy: config.retryPolicy ?? DefaultRetryPolicy})
        this.client = new StreamClient({registry: this.registry})
        this.streamOffsets = {}
    }

    private async getStreamOffset(streamId: string): Promise<Offset> {
        if (!this.streamOffsets[streamId]) {
            const rawOffset = (await this.registry.findOffset(streamId, 1))
            const offset = typeof rawOffset == "string" ? Offset.fromString(rawOffset) : rawOffset
            this.setStreamOffset(streamId, offset)
        }
        return this.streamOffsets[streamId]
    }

    private setStreamOffset(streamId: string, offset: Offset) {
        this.streamOffsets[streamId] = offset
    }

 
    async fetchStreamEvents(streamId: string, batchSize: number): Promise<any[]> {
        const dir = "next"
        try { 
            const offset = await this.getStreamOffset(streamId)
            const streamEvents = await this.client.fetchEvents(streamId, offset, batchSize, dir)
            this.setStreamOffset(streamId, streamEvents[streamEvents.length-1].offset)
             return streamEvents.map((event) => {
                 return parseEvent(event)
            })

         } catch (e) {
             console.log(e)
             return []
         }
    } 
}



export function parseEvent(event: StreamEvent): any {
    const value = JSON.parse(event.payload.toString())
    return {
        "id": event.offset.toString(),
        "undo": event.undo,
      //  "offset": event.offset.toString(),
        "timestamp": event.timestamp,
        "value": value,
    }
}