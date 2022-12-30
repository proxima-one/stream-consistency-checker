import { readdirSync, readFileSync } from "fs"
import {outputFileSync } from "fs-extra"


export function writeStreamToFile(directory: string, folder: string, streamName: string, events: any[]) {
    if (events && events[0]) {
        const fileName =  directory +  "/streams/" + folder + "/" + streamName + "/" + events[0]["id"]
        const rawString = JSON.stringify(events)
        outputFileSync(fileName, rawString)
    } else {
        console.log(events)
    }
}

export function getSortedFiles(directory: string, folder: string, streamName: string): string[] {
    const files = readdirSync(directory +  "/streams/" + folder + "/" + streamName)
    return files.sort((a, b) => {
        const aComp = Number(a.split("-")[0])
        const bComp = Number(b.split("-")[0])
        if (aComp < bComp) return -1;
        if (bComp < aComp) return 1;
        return 0
    })
}

export function readStreamFromFile(opts: {fileName?: string, directory?: string, folder?: string, streamName?: string, name?: string}): any[] {
    const fileName =opts.fileName ? opts.fileName :  "./" + opts.folder + "/" + opts.streamName + "/" + opts.name
    const rawString = readFileSync(fileName)
    return JSON.parse(rawString.toString())
}

export function compareEventWithDiff(oldEvent: Event, newEvent: Event): EventDiff {
    const oldEventHeight = Number(oldEvent.id.split("-")[0])
    const newEventHeight = Number(newEvent.id.split("-")[0]) - 2 //TODO new and old height differ by 2
    const id =  oldEventHeight < newEventHeight ? oldEvent.id : newEvent.id
    let diffType = "none"
    let undoBool = oldEvent.undo == newEvent.undo
    let valueBool = JSON.stringify(oldEvent.value) == JSON.stringify(newEvent.value)
    if ( oldEventHeight < newEventHeight) {
       diffType =  "missing" 
       return {
        id: id,
        type: diffType,
        oldEvent: oldEvent,
        newEvent: newEvent
     }
    }

    if (oldEventHeight > newEventHeight) {
        diffType =  "extra" 
        return {
            id: newEvent.id,
            type: diffType,
            newEvent: newEvent
        }
    }

    if (oldEventHeight == newEventHeight && (!undoBool || !valueBool) ) {  
        diffType ==  "difference" 
    }
    return {
        id: id,
        type: diffType,
        oldEvent: oldEvent,
        newEvent: newEvent
    }
}

export interface Event {
    id: string,
    timestamp: string,
    undo: boolean,
    value: any
}

export interface EventDiff {
    id: string,
    type: string,
    oldEvent?: Event,
    newEvent?: Event
}

