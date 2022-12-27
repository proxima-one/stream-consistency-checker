import { NewClient } from "./new-client";
import { OldClient } from "./old-client";
import { writeStreamToFile } from "./stream-utils";

//streams to compare
const oldClientURI = "streams.cluster.amur.proxima.one:443";
const newClientConfig = {
  endpoint: "https://streams.api.proxima.one",
};

const DIRECTORY = "."


const batchSize = 1000;
const maxEvents = 100000000; // 100 hundred million

async function main() {
    const domainEvents = {
      old: "v5.domain-events.polygon-mumbai.mangrove.streams.proxima.one",
      //new: "proxima.mangrove.polygon-mumbai.domain-events.0_1",
    };
    const strategies = {
      old: "v4.multi-user-strategies.polygon-mumbai.mangrove.streams.proxima.one",
      //new: "proxima.mangrove.polygon-mumbai.multi-user-strategies.0_1"
    };
    const newTokens = {
      old: "new-tokens.polygon-mumbai.fungible-token.streams.proxima.one",
      //new: "proxima.erc20.polygon-mumbai.events.1_0",
    };
    const ethMain = {
      old: "v2.eth-main.fungible-token.streams.proxima.one",
      //new: "proxima.erc20.eth-main.events.1_0"
    };

    const ethGoerli = {
      old: "v1.eth-goerli.fungible-token.streams.proxima.one",
      //new: "proxima.erc20.eth-goerli.events.1_0"
    };

    const polygonMumbai = {
      old: "v1.polygon-mumbai.fungible-token.streams.proxima.one",
      //new: "proxima.erc20.polygon-mumbai.events.1_0"
    };

    const polygonNewTokens = {
      old: "v1.new-tokens.polygon-mumbai.fungible-token.streams.proxima.one",
      //new: "proxima.ft.polygon-mumbai.new-tokens.0_2"
    };
    return await Promise.all([
        streamConsistencyCheck("mangrove-domain-events", domainEvents, maxEvents),
        streamConsistencyCheck("mangrove-strategies", strategies, maxEvents),
        streamConsistencyCheck("mangrove-new-tokens", newTokens, maxEvents),
        streamConsistencyCheck("eth-main", ethMain, maxEvents),
        streamConsistencyCheck("eth-goerli", ethGoerli, maxEvents),
        streamConsistencyCheck("polygonMumbai", polygonMumbai, maxEvents),
        streamConsistencyCheck("polygonNewTokens", polygonNewTokens, maxEvents)
    ])
}

async function streamConsistencyCheck(
  folderName: string,
  streams: { old: string; new?: string },
  limit: number = 100000000
) {
  const oldClient = new OldClient(oldClientURI);
  const newClient = new NewClient(newClientConfig);
  const clients = { old: oldClient, new: newClient };
  let eventsProcessed = 0;
  try {
    while (true && limit > eventsProcessed) {
      const oldEvents = await clients.old.fetchStreamEvents(
        streams.old,
        batchSize
      );
      writeStreamToFile(DIRECTORY, folderName, streams.old, oldEvents);


      if (streams.new) {
        const newEvents = await clients.new.fetchStreamEvents(
          streams.new,
          batchSize
        );
        writeStreamToFile(DIRECTORY, folderName, streams.new, newEvents);
      }
      if (oldEvents.length < batchSize) {
        console.log(folderName + " Finished: " + eventsProcessed + oldEvents.length)
        return
      }
      eventsProcessed += batchSize;
      console.log(folderName + " Events Processed: " + eventsProcessed)
    }
  } catch (e) {
    console.log(e);
  } finally {
      return;
  }
}


main().then(()=> {
    console.log("Finished")
})