import { NewClient } from "./new-client";
import { OldClient } from "./old-client";
import { writeStreamToFile } from "./stream-utils";

//streams to compare
const oldClientURI = "streams.cluster.amur.proxima.one:443";
const oldClient = new OldClient(oldClientURI);
const newClientConfig = {
  endpoint: "https://streams.api.proxima.one",
};

const newClient = new NewClient(newClientConfig);
const clients = { old: oldClient, new: newClient };
const batchSize = 1000;
const maxEvents = 1000000000; // 100 hundred million

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
    streamConsistencyCheck("mangrove-domain-events", domainEvents, maxEvents);
    streamConsistencyCheck("mangrove-strategies", strategies, maxEvents);
    streamConsistencyCheck("mangrove-new-tokens", newTokens, maxEvents);
    streamConsistencyCheck("eth-main", ethMain, maxEvents);
    streamConsistencyCheck("eth-goerli", ethGoerli, maxEvents);
    streamConsistencyCheck("polygonMumbai", polygonMumbai, maxEvents);
    streamConsistencyCheck("polygonNewTokens", polygonNewTokens, maxEvents);

async function streamConsistencyCheck(
  folderName: string,
  streams: { old: string; new?: string },
  limit: number = 1000000000
) {
  let eventsProcessed = 0;
  try {
    while (true && limit > eventsProcessed) {
      const oldEvents = await clients.old.fetchStreamEvents(
        streams.old,
        batchSize
      );
      writeStreamToFile(folderName, streams.old, oldEvents);

      if (streams.new) {
        const newEvents = await clients.new.fetchStreamEvents(
          streams.new,
          batchSize
        );
        writeStreamToFile(folderName, streams.new, newEvents);
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
