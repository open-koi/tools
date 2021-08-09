import dotenv from "dotenv";
dotenv.config();

import { Common, Vote, BundlerPayload, arweave } from "./common";
import { JWKInterface } from "arweave/node/lib/wallet";
import * as arweaveUtils from "arweave/node/lib/utils";
import { smartweave } from "smartweave";
import { readFile } from "fs/promises";
import Datastore from "nedb-promises";
import axios, { AxiosResponse } from "axios";
import redis, { RedisClient } from "redis";
//@ts-ignore
import * as swicw from "swicw";

interface VoteState {
  id: number;
  type: string;
  voted: [];
  stakeAmount: number;
  yays: number;
  nays: number;
  bundlers: any;
  start: number;
  end: number;
}

export const URL_GATEWAY_LOGS = "https://gatewayv2.koi.rocks/logs";
const SERVICE_SUBMIT = "/submit-vote";

export class Node extends Common {
  db?: Datastore;
  totalVoted = -1;
  receipts: Array<any> = [];
  redisClient?: RedisClient;

  /**
   * Asynchronously load a wallet from a UTF8 JSON file
   * @param file Path of the file to be loaded
   * @returns JSON representation of the object
   */
  async loadFile(file: string): Promise<any> {
    const data = await readFile(file, "utf8");
    return JSON.parse(data);
  }

  /**
   * Loads wallet for node Simulator key from file path and initialize ndb.
   * @param walletFileLocation Wallet key file location
   * @returns Key as an object
   */
  async nodeLoadWallet(
    walletFileLocation: string
  ): Promise<JWKInterface | undefined> {
    const jwk = await this.loadFile(walletFileLocation);
    await this.loadWallet(jwk);
    const voteId = await this._activeVote();
    this.db = Datastore.create({
      filename: "my-db.db",
      autoload: true
    });
    const count = await this.db.count({});
    if (count == 0) {
      const data = {
        totalVoted: voteId,
        receipt: []
      };

      await this.db.insert(data);

      this.totalVoted = data.totalVoted;
    } else {
      const data: Array<any> = await this.db.find({});
      this.totalVoted = data[0].totalVoted;
      this.receipts.push(data[0].receipt);
    }
    return this.wallet;
  }

  /**
   * Submit vote to bundle server or direct to contract
   * @param arg Object with direct, voteId, and useVote
   * @returns Transaction ID
   */
  async vote(arg: Vote): Promise<any> {
    const userVote: any = await this.validateData(arg.voteId);
    if (userVote == null) {
      this.totalVoted += 1;
      await this._db();
      return { message: "VoteTimePassed" };
    }

    const input = {
      function: "vote",
      voteId: arg.voteId,
      userVote: userVote
    };

    let receipt;
    let tx;
    if (arg.direct) tx = await this._interactWrite(input);
    else {
      const caller = await this.getWalletAddress();

      // Vote must be a string when indirect voting through bundler
      input.userVote = userVote.toString();

      const payload: BundlerPayload = {
        vote: input,
        senderAddress: caller
      };

      receipt = await this._bundlerNode(payload);
    }

    if (tx) {
      this.totalVoted += 1;
      await this._db();
      return { message: "justVoted" };
    }

    if (!receipt) return null;

    if (this.db !== undefined && receipt.status === 200) {
      if (receipt.data.message == "success") {
        this.totalVoted += 1;
        const data = receipt.data.receipt;
        const id = await this._db();
        await this.db.update({ _id: id }, { $push: { receipt: data } });
        this.receipts.push(data);
        return { message: "success" };
      } else if (receipt.data.message == "duplicate") {
        this.totalVoted += 1;
        await this._db();
        return { message: "duplicatedVote" };
      }
    } else {
      this.totalVoted += 1;
      await this._db();
      return { message: receipt.data.message };
    }

    // Status 200, but message doesn't match.
    return null;
  }

  /**
   * propose a tafficLog for vote
   * @param arg
   * @returns object arg.gateway(trafficlog orginal gateway id) and arg.stakeAmount(min required stake to vote)
   */
  async submitTrafficLog(arg: any): Promise<string> {
    const TLTxId = await this._storeTrafficLogOnArweave(arg.gateWayUrl);

    const input = {
      function: "submitTrafficLog",
      gateWayUrl: arg.gateWayUrl,
      batchTxId: TLTxId,
      stakeAmount: arg.stakeAmount
    };
    return this._interactWrite(input);
  }

  /**
   * Triggers proposal rank in contract
   * @returns Transaction ID
   */
  rankProposal(): Promise<string> {
    const input = {
      function: "rankProposal"
    };
    return this._interactWrite(input);
  }

  /**
   * Interact with contract to add the votes
   * @param arg Batch data
   * @returns Transaction ID
   */
  batchAction(arg: any): Promise<string> {
    // input object that pass to contract
    const input = {
      function: "batchAction",
      batchFile: arg.batchFile,
      voteId: arg.voteId,
      bundlerAddress: arg.bundlerAddress
    };

    // interact with contract function batchAction which adds all votes and update the state
    return this._interactWrite(input);
  }

  /**
   * Propose a stake slashing
   * @returns
   */
  async proposeSlash(): Promise<void> {
    const state = await this.getContractState();
    const votes = state.votes;
    const currentTrafficLogs =
      state.stateUpdate.trafficLogs.dailyTrafficLog.filter(
        (proposedLog: {
          block: number;
          proposedLogs: [];
          isRanked: boolean;
          isDistributed: boolean;
        }) => proposedLog.block == state.stateUpdate.trafficLogs.open
      );
    for (const proposedLogs of currentTrafficLogs) {
      const currentProposedLogsVoteId = proposedLogs.voteId;
      for (let i = 0; i < this.receipts.length - 1; i++) {
        if (this.receipts[i].vote.vote.voteId === currentProposedLogsVoteId) {
          const vote = votes[currentProposedLogsVoteId];
          if (!vote.voted.includes(this.wallet)) {
            const input = {
              function: "proposeSlash",
              receipt: this.receipts[i]
            };
            await this._interactWrite(input);
          }
        }
      }
    }
  }

  /**
   * Triggers distribute reward function
   * @returns Transaction ID
   */
  async distributeDailyRewards(): Promise<string> {
    const input = {
      function: "distributeRewards"
    };
    return this._interactWrite(input);
  }

  /**
   * Validate traffic log by comparing traffic log from gateway and arweave storage
   * @param voteId Vote id which is belongs for specific proposalLog
   * @returns Whether data is valid
   */
  async validateData(voteId: number): Promise<boolean | null> {
    const state: any = await this.getContractState();
    const trafficLogs = state.stateUpdate.trafficLogs;
    const currentTrafficLogs = trafficLogs.dailyTrafficLog.find(
      (trafficLog: any) => trafficLog.block === trafficLogs.open
    );
    const proposedLogs = currentTrafficLogs.proposedLogs;
    const proposedLog = proposedLogs.find((log: any) => log.voteId === voteId);
    // lets assume we have one gateway id for now.
    //let gateWayUrl = proposedLog.gatWayId;

    if (proposedLog === undefined) return null;

    const gatewayTrafficLogs = await this._getTrafficLogFromGateWay(
      URL_GATEWAY_LOGS
    );
    const gatewayTrafficLogsHash = await this._hashData(
      gatewayTrafficLogs.data.summary
    );

    const bundledTrafficLogs = (await arweave.transactions.getData(
      proposedLog.TLTxId,
      { decode: true, string: true }
    )) as string;

    const bundledTrafficLogsParsed = JSON.parse(bundledTrafficLogs);
    const bundledTrafficLogsParsedHash = await this._hashData(
      bundledTrafficLogsParsed
    );

    return gatewayTrafficLogsHash === bundledTrafficLogsParsedHash;
  }

  /**
   * Loads redis client
   */
  loadRedisClient(): void {
    if (!process.env.REDIS_IP || !process.env.REDIS_PORT) {
      throw Error("CANNOT READ REDIS IP OR PORT FROM ENV");
    } else {
      this.redisClient = redis.createClient({
        host: process.env.REDIS_IP,
        port: parseInt(process.env.REDIS_PORT),
        password: process.env.REDIS_PASSWORD
      });

      this.redisClient.on("error", function (error) {
        console.error("redisClient " + error);
      });
    }
  }

  /**
   * Recalculates the predicted state based on the pending transactions
   * @param wallet
   * @param latestContractState
   * @param redisClient
   * @returns
   */
  async recalculatePredictedState(
    wallet: any,
    latestContractState: any,
    redisClient: any
  ): Promise<any> {
    if (!redisClient) redisClient = this.redisClient;
    if (!wallet) wallet = this.wallet;
    if (!latestContractState) latestContractState = await super._readContract();
    await this._checkPendingTransactionStatus(latestContractState);
    const pendingStateArrayStr = await this.redisGetAsync("pendingStateArray");
    if (!pendingStateArrayStr) {
      console.error("No pending state found");
      return;
    }
    const pendingStateArray = JSON.parse(pendingStateArrayStr);
    let finalState = { state: latestContractState };
    let contract = null;
    let from = null;
    try {
      contract = await smartweave.loadContract(arweave, this.contractId);
      from = await arweave.wallets.getAddress(wallet);
    } catch (e) {
      console.error(e);
    }
    for (let i = 0; i < pendingStateArray.length; i++) {
      try {
        const pendingTx = pendingStateArray[i];
        console.log(
          `Pending transaction ${i + 1} (${pendingTx.status}) ${pendingTx.txId}`
        );
        if (i == 0) {
          if (pendingStateArray[i].signedTx) {
            finalState = await this.registerDataDryRun(
              pendingStateArray[i].txId,
              pendingStateArray[i].owner,
              pendingStateArray[i].signedTx,
              latestContractState,
              contract
            );
            continue;
          }
          finalState = await smartweave.interactWriteDryRun(
            arweave,
            wallet,
            this.contractId,
            pendingStateArray[i].input,
            undefined,
            undefined,
            undefined,
            latestContractState,
            from,
            contract
          );
          break;
        } else {
          if (pendingStateArray[i].signedTx) {
            finalState = await this.registerDataDryRun(
              pendingStateArray[i].txId,
              pendingStateArray[i].owner,
              pendingStateArray[i].signedTx,
              finalState.state,
              contract
            );
            continue;
          }
          finalState = await smartweave.interactWriteDryRun(
            arweave,
            wallet,
            this.contractId,
            pendingStateArray[i].input,
            undefined,
            undefined,
            undefined,
            finalState.state,
            from,
            contract
          );
          // console.timeEnd("Time this");
        }
      } catch (e) {
        console.error(e);
      }
    }
    console.log(
      "Final predicted state:",
      (finalState as any).type,
      (finalState as any).result
    );
    if (finalState.state)
      await this.redisSetAsync(
        "ContractPredictedState",
        JSON.stringify(finalState.state)
      );
  }

  /**
   * internal function, writes to contract. Used explicitly for signed transaction received from UI, uses redis
   * @param txId
   * @param owner
   * @param tx
   * @param state
   * @returns
   */
  async registerDataDryRun(
    txId: any,
    owner: any,
    tx: any,
    state: any,
    contract: any
  ): Promise<any> {
    const input = {
      function: "registerData",
      txId: txId,
      owner: owner
    };
    const fromParam = await arweave.wallets.ownerToAddress(tx.owner);
    // let currentFinalPredictedState=await redisGetAsync("TempPredictedState")
    const finalState = await smartweave.interactWriteDryRunCustom(
      arweave,
      tx,
      this.contractId,
      input,
      state,
      fromParam,
      null
    );
    console.log(
      "Semi FINAL Predicted STATE for registerData",
      finalState.state ? finalState.state.registeredRecord : "NULL"
    );
    if (finalState.type != "exception") {
      await this.redisSetAsync(
        "ContractPredictedState",
        JSON.stringify(finalState.state)
      );
      return finalState;
    } else {
      console.error("EXCEPTION", finalState);
    }
    return state;
    // this._interactWrite(input)
  }

  /**
   * Store data in Redis async
   * @param key Redis key of data
   * @param value String to store in redis
   * @returns
   */
  redisSetAsync(key: any, value: string): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.redisClient === undefined) reject("Redis not connected");
      else
        this.redisClient.set(key, value, (err) => {
          err ? reject(err) : resolve();
        });
    });
  }

  /**
   * Get data from Redis async
   * @param key Redis key of data
   * @returns Data as a string, null if no such key exists
   */
  redisGetAsync(key: any): Promise<string | null> {
    return new Promise((resolve, reject) => {
      if (this.redisClient === undefined) reject("Redis not connected");
      else
        this.redisClient.get(key, (err, res) => {
          err ? reject(err) : resolve(res);
        });
    });
  }

  // Protected functions

  /**
   * internal function, writes to contract. Overrides common._interactWrite, uses redis
   * @param input
   * @returns Transaction ID
   */
  protected async _interactWrite(input: any): Promise<string> {
    const wallet = this.wallet === undefined ? "use_wallet" : this.wallet;

    if (!this.redisClient)
      return smartweave.interactWrite(arweave, wallet, this.contractId, input);

    // Adding the dryRun logic
    const pendingStateArrayStr = await this.redisGetAsync("pendingStateArray");
    const pendingStateArray = !pendingStateArrayStr
      ? []
      : JSON.parse(pendingStateArrayStr);

    const latestContractState = await this._readContract();

    const txId = await smartweave.interactWrite(
      arweave,
      wallet,
      this.contractId,
      input
    );
    pendingStateArray.push({
      status: "pending",
      txId: txId,
      input: input
      // dryRunState:response.state,
    });
    await this.redisSetAsync(
      "pendingStateArray",
      JSON.stringify(pendingStateArray)
    );
    await this.recalculatePredictedState(
      wallet,
      latestContractState,
      this.redisClient
    );

    return txId;
  }

  /**
   * Read contract latest state
   * @returns Contract
   */
  protected async _readContract(): Promise<any> {
    if (process.env.NODE_MODE !== "service") {
      try {
        const response = await axios.get(
          "https://devbundler.openkoi.com:8888/state/current"
        );
        if (response.data) return response.data;
      } catch (e) {
        console.error("Cannot retrieve from bundler:", e);
      }
    } else {
      if (this.redisClient) {
        // First Attempt to retrieve the ContractPredictedState from redis
        const stateStr = await this.redisGetAsync("ContractPredictedState");
        if (stateStr !== null) {
          const state = JSON.parse(stateStr);
          if (state) {
            const balances = state["balances"];
            if (balances !== undefined && balances !== null) return state;
          }
        }
      }

      // Next Attempt to retrieve ContractCurrentState from redis (Stored when data was successfully retrieved from KYVE)
      if (this.redisClient) {
        const stateStr = await this.redisGetAsync("ContractCurrentState");
        if (stateStr !== null) {
          const state = JSON.parse(stateStr);
          if (state) {
            const balances = state["balances"];
            if (balances !== undefined && balances !== null) return state;
          }
        }
      }
    }
    // If no state found on the cache retrieve the state in sync from swicw
    const swicwState = await swicw.readContract(arweave, this.contractId);
    if (swicwState) return swicwState;

    // Fallback to smartweave
    return smartweave.readContract(arweave, this.contractId);
  }

  // Private functions
  /**
   * Read the data and update
   * @returns Database document ID
   */
  private async _db(): Promise<string | null> {
    if (this.db === undefined) return null;

    const dataB: any = await this.db.find({});
    const id: string = dataB[0]._id;
    const receipt = dataB[0].receipt; // dataB is forced to any to allow .receipt
    await this.db.update(
      { _id: id },
      {
        totalVoted: this.totalVoted,
        receipt: receipt
      }
    );
    return id;
  }

  /**
   * Get the latest state
   * @returns  Active vote Id
   */
  private async _activeVote(): Promise<number> {
    const state = await this.getContractState();
    const activeVotes = state.votes.find(
      (vote: VoteState) => vote.end == state.stateUpdate.trafficLogs.close
    );
    if (activeVotes !== undefined) {
      return activeVotes.id - 1;
    } else {
      return state.votes.length - 1;
    }
  }

  /**
   * Submits a payload to server
   * @param payload Payload to be submitted
   * @returns Result as a promise
   */
  private async _bundlerNode(
    payload: BundlerPayload
  ): Promise<AxiosResponse<any> | null> {
    const sigResult = await this.signPayload(payload);
    return sigResult !== null
      ? await axios.post(this.bundlerUrl + SERVICE_SUBMIT, sigResult)
      : null;
  }

  /**
   * Get traffic logs from gateway
   * @param path Gateway url
   * @returns Result as a promise
   */
  private _getTrafficLogFromGateWay(path: string): Promise<any> {
    return axios.get(path);
  }

  /**
   *
   * @param gateWayUrl
   * @returns
   */
  private async _storeTrafficLogOnArweave(
    gateWayUrl: string
  ): Promise<string | null> {
    const trafficLogs = await this._getTrafficLogFromGateWay(gateWayUrl);
    return await this.postData(trafficLogs.data.summary);
  }

  /**
   * Read contract latest state
   * @param data Data to be hashed
   * @returns Hex string
   */
  private async _hashData(data: any): Promise<string> {
    const dataInString = JSON.stringify(data);
    const dataIn8Array = arweaveUtils.stringToBuffer(dataInString);
    const hashBuffer = await arweave.crypto.hash(dataIn8Array);
    const hashArray = Array.from(new Uint8Array(hashBuffer)); // convert buffer to byte array
    const hashHex = hashArray
      .map((b) => b.toString(16).padStart(2, "0"))
      .join(""); // convert bytes to hex string
    return hashHex;
  }

  /**
   *
   * @param redisClient
   * @returns
   */
  private async _checkPendingTransactionStatus(
    latestContractState: any
  ): Promise<any> {
    const registeredRecords = latestContractState
      ? latestContractState.registeredRecord
      : {};
    const pendingStateArrayStr = await this.redisGetAsync("pendingStateArray");
    if (!pendingStateArrayStr) {
      console.error("No pending state found");
      return;
    }
    let pendingStateArray = JSON.parse(pendingStateArrayStr);
    for (let i = 0; i < pendingStateArray.length; i++) {
      try {
        const arweaveTxStatus = await arweave.transactions.getStatus(
          pendingStateArray[i].txId
        );
        if (
          arweaveTxStatus.status != 202 &&
          pendingStateArray[i].txId in registeredRecords
        ) {
          pendingStateArray[i].status = "Not pending";
        }
      } catch (e) {
        console.error(e);
        pendingStateArray[i].status = "Not pending";
      }
    }
    pendingStateArray = pendingStateArray.filter((e: any) => {
      return e.status == "pending";
    });
    await this.redisSetAsync(
      "pendingStateArray",
      JSON.stringify(pendingStateArray)
    );
  }
}

module.exports = { Node, URL_GATEWAY_LOGS };
