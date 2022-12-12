import {StreamReader, ProximaStreamsClient, State, Transition, Event} from "@proximaone/stream-client-js"
  

export class OldClient {
    proximaClient: ProximaStreamsClient; 
    streamReaders: Record<string, StreamReader>;

    public constructor(uri: string = "streams.proxima.one:443") {
        this.proximaClient = new ProximaStreamsClient(uri)
        this.streamReaders = {}
    }

    private getStreamReader(streamId: string): StreamReader {
        if (!this.streamReaders[streamId]) {
            const streamReader = new StreamReader(this.proximaClient, streamId, State.genesis);
            this.setStreamReader(streamId, streamReader)
        }
        return this.streamReaders[streamId]
    }

    private setStreamReader(streamId: string, reader: StreamReader) {
        this.streamReaders[streamId] = reader
    }

    async fetchStreamEvents(streamId: string, batchSize: number): Promise<any[]> {
        const streamReader = this.getStreamReader(streamId)
        const transitions = await streamReader.tryRead(batchSize, 10 * 60 * 1000);
        this.setStreamReader(streamId, streamReader)
        return transitions.map((transition: Transition) => {
            return parseEvent(transition)
        })
    }
}

  export function parseEvent(tr: Transition): any {
    const value = JSON.parse(tr.event.payload.toString())
    return {
       "id": tr.newState.id.toString(),
        "undo": tr.event.undo,
        "timestamp": tr.event.timestamp,
        "value": value,
    }
}