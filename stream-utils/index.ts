import {outputFileSync } from "fs-extra"


export function writeStreamToFile(folder: string, streamName: string, events: any[]) {
    if (events[0]) {
        const fileName = "./streams/" + folder + "/" + streamName + "/" + events[0]["id"]
        const rawString = JSON.stringify(events)
        outputFileSync(fileName, rawString)
    }
}

// export function readStreamFromFile(folder: string, streamName: string, lastRead?: string): any[] {
//     const fileName = "./" + folder + "/" + streamName + "/" + events[-1].id
    
// }

