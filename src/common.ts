import axios, { AxiosResponse } from "axios";
import Arweave from "arweave";
import { JWKInterface } from "arweave/node/lib/wallet";
import * as arweaveUtils from "arweave/node/lib/utils";
import Transaction from "arweave/node/lib/transaction";
import { smartweave } from "smartweave";
import { Query } from "@kyve/query";

//@ts-ignore // Needed to allow implicit any here
import { generateKeyPair, getKeyPairFromMnemonic } from "human-crypto-keys";
//@ts-ignore
import { pem2jwk } from "pem-jwk";

export interface BundlerPayload {
  data?: any;
  signature?: string; // Data signed with private key
  owner?: string; // Public modulus, can be used to verifiably derive address
  senderAddress?: string; //@deprecated // Use owner instead
  vote?: Vote; //@deprecated // Use data instead
}

export interface Vote {
  voteId: number;
  direct?: string;
}

export interface RegistrationData {
  url: string;
  timestamp: number;
}

const HOST_GATEWAY = "arweave.net";
const URL_ARWEAVE_INFO = `https://${HOST_GATEWAY}/info`;
const URL_ARWEAVE_GQL = `https://${HOST_GATEWAY}/graphql`;

const BLOCK_TEMPLATE = `
  pageInfo {
    hasNextPage
  }
  edges {
    cursor
    node {
      id anchor signature recipient
      owner { address key }
      fee { winston ar }
      quantity { winston ar }
      data { size type }
      tags { name value }
      block { id timestamp height previous }
      parent { id }
    }
  }`;

export const arweave = Arweave.init({
  host: HOST_GATEWAY,
  protocol: "https",
  port: 443
});

export const BUNDLER_NODES = "/nodes";

/**
 * Tools for interacting with the koi network
 */
export class Common {
  wallet?: JWKInterface;
  mnemonic?: string;
  address?: string;
  contractId: string;
  bundlerUrl: string;

  constructor(
    bundlerUrl = "https://bundler.openkoi.com:8888",
    contractId = "cETTyJQYxJLVQ6nC3VxzsZf1x2-6TW2LFkGZa91gUWc"
  ) {
    this.bundlerUrl = bundlerUrl;
    this.contractId = contractId;
    console.log(
      "Initialized Koii Tools for true ownership and direct communication using version ",
      this.contractId
    );
  }

  /**
   * Generates wallet optionally with a mnemonic phrase
   * @param use_mnemonic [false] Flag for enabling mnemonic phrase wallet generation
   */
  async generateWallet(use_mnemonic = false): Promise<Error | true> {
    let key: JWKInterface, mnemonic: string | undefined;
    if (use_mnemonic === true) {
      mnemonic = await this._generateMnemonic();
      key = await this._getKeyFromMnemonic(mnemonic);
    } else key = await arweave.wallets.generate();

    if (!key) throw Error("failed to create wallet");

    this.mnemonic = mnemonic;
    this.wallet = key;
    await this.getWalletAddress();
    return true;
  }

  /**
   * Loads arweave wallet
   * @param source object to load from, JSON or JWK, or mnemonic key
   */
  async loadWallet(source: any): Promise<JWKInterface> {
    switch (typeof source) {
      case "string":
        this.wallet = await this._getKeyFromMnemonic(source);
        break;
      default:
        this.wallet = source as JWKInterface;
    }

    await this.getWalletAddress();
    return this.wallet;
  }

  /**
   * Manually set wallet address
   * @param walletAddress Address as a string
   * @returns Wallet address
   */
  setWallet(walletAddress: string): string {
    if (!this.address) this.address = walletAddress;
    return this.address;
  }

  /**
   * Uses koi wallet to get the address
   * @returns Wallet address
   */
  async getWalletAddress(): Promise<string> {
    if (typeof this.address !== "string")
      this.address = await arweave.wallets.jwkToAddress(this.wallet);
    return this.address;
  }

  /**
   * Get and set arweave balance
   * @returns Balance as a string if wallet exists, else undefined
   */
  async getWalletBalance(): Promise<number> {
    if (!this.address) return 0;
    const winston = await arweave.wallets.getBalance(this.address);
    const ar = arweave.ar.winstonToAr(winston);
    return parseFloat(ar);
  }

  /**
   * Gets koi balance from cache
   * @returns Balance as a number
   */
  async getKoiBalance(): Promise<number> {
    const state = await this.getContractState();
    if (this.address !== undefined && this.address in state.balances)
      return state.balances[this.address];
    return 0;
  }

  /**
   * Gets the current contract state
   * @returns Current KOI system state
   */
  getContractState(): Promise<any> {
    return this._readContract();
  }
  
  /**
   * Gets the current arweave network state
   * @returns Current Arweave system state
   */
  getContractState(): Promise<any> {
    return this.arweave.network.getInfo();
  }

  /**
   * Get contract state
   * @param id Transaction ID
   * @returns State object
   */
  async getTransaction(id: string): Promise<Transaction> {
    return arweave.transactions.get(id);
  }

  /**
   * Get block height
   * @returns Block height maybe number
   */
  async getBlockHeight(): Promise<any> {
    const info = await getArweaveNetInfo();
    return info.data.height;
  }

  /**
   * Get the NFT state from arweave, this should be the initial state
   * @param txId Transaction ID of the NFT
   * @returns The NFT state object
   */
  async readNftState(txId: string): Promise<any> {
    try { 
      const response = await axios.get(
        `https://bundler.openkoi.com:8888/state/getNFTState?tranxId=${txId}`
      );
      return response.data;
    } catch (err) {
      console.log("ERRPR",err)
      if (err) console.error('error fetching NFT data from bundler for ' + txId);
      return smartweave.readContract(arweave, txId);
    }
  }

  /**
   * Interact with contract to stake
   * @param qty Quantity to stake
   * @returns Transaction ID
   */
  stake(qty: number): Promise<string> {
    if (!Number.isInteger(qty))
      throw Error('Invalid value for "qty". Must be an integer');
    const input = {
      function: "stake",
      qty: qty
    };

    return this._interactWrite(input);
  }

  /**
   * Interact with contract to withdraw
   * @param qty Quantity to transfer
   * @returns Transaction ID
   */
  withdraw(qty: number): Promise<string> {
    if (!Number.isInteger(qty))
      throw Error('Invalid value for "qty". Must be an integer');
    const input = {
      function: "withdraw",
      qty: qty
    };

    return this._interactWrite(input);
  }

  /**
   * Interact with contract to transfer koi
   * @param qty Quantity to transfer
   * @param target Receiver address
   * @returns Transaction ID
   */
  async transfer(qty: number, target: string, token: string): Promise<string> {
    const input = {
      function: "transfer",
      qty: qty,
      target: target
    };
    switch (token) {
      case "AR": {
        const transaction = await arweave.createTransaction(
          { target: target, quantity: arweave.ar.arToWinston(qty.toString()) },
          this.wallet
        );
        await arweave.transactions.sign(transaction, this.wallet);
        await arweave.transactions.post(transaction);
        return transaction.id;
      }
      case "KOI": {
        const txid = await this._interactWrite(input);
        return txid;
      }

      default: {
        throw Error("token or coin ticker doesn't exist");
      }
    }
  }

  /**
   * Mint koi
   * @param arg object arg.targetAddress(receiver address) and arg.qty(amount to mint)
   * @returns Transaction ID
   */
  mint(arg: any): Promise<string> {
    const input = {
      function: "mint",
      qty: arg.qty,
      target: arg.targetAddress
    };
    return this._interactWrite(input);
  }

  /**
   * Interact with contract to register data
   * @param txId It has batchFile/value(string) and stake amount/value(int) as properties
   * @param ownerId String container the owner ID
   * @returns Transaction ID
   */
  registerData(txId: string, ownerId = ""): Promise<string> {
    const input = {
      function: "registerData",
      txId: txId,
      owner: ownerId
    };
    return this._interactWrite(input);
  }

  /**
   * Sign transaction
   * @param tx Transaction to be signed
   * @returns signed Transaction
   */
  async signTransaction(tx: Transaction): Promise<any> {
    try {
      //const wallet = this.wallet;
      // Now we sign the transaction
      await arweave.transactions.sign(tx, this.wallet);
      // After is signed, we send the transaction
      //await exports.arweave.transactions.post(transaction);
      return tx;
    } catch (err) {
      return null;
    }
  }

  /**
   * Get transaction data from Arweave
   * @param txId Transaction ID
   * @returns Transaction
   */
  nftTransactionData(txId: string): Promise<Transaction> {
    return arweave.transactions.get(txId);
  }

  /**
   * Sign payload
   * @param payload Payload to sign
   * @returns Signed payload with signature
   */
  async signPayload(payload: BundlerPayload): Promise<BundlerPayload | null> {
    if (this.wallet === undefined) return null;
    const data = payload.data || payload.vote || null;
    const jwk = this.wallet;
    const publicModulus = jwk.n;
    const dataInString = JSON.stringify(data);
    const dataIn8Array = arweaveUtils.stringToBuffer(dataInString);
    const rawSignature = await arweave.crypto.sign(jwk, dataIn8Array);
    payload.signature = arweaveUtils.bufferTob64Url(rawSignature);
    payload.owner = publicModulus;
    return payload;
  }

  /**
   * Verify signed payload
   * @param payload
   * @returns Verification result
   */
  async verifySignature(payload: any): Promise<boolean> {
    const data = payload.data || payload.vote || null;
    const rawSignature = arweaveUtils.b64UrlToBuffer(payload.signature);
    const dataInString = JSON.stringify(data);
    const dataIn8Array = arweaveUtils.stringToBuffer(dataInString);
    return await arweave.crypto.verify(
      payload.owner,
      dataIn8Array,
      rawSignature
    );
  }

  /**
   * Posts data to Arweave
   * @param data
   * @returns Transaction ID
   */
  async postData(data: any): Promise<string | null> {
    // TODO: define data interface
    const wallet = this.wallet;
    const transaction = await arweave.createTransaction(
      {
        data: Buffer.from(JSON.stringify(data, null, 2), "utf8")
      },
      wallet
    );

    // Now we sign the transaction
    await arweave.transactions.sign(transaction, wallet);
    const txId = transaction.id;

    // After is signed, we send the transaction
    const response = await arweave.transactions.post(transaction);

    if (response.status === 200) return txId;

    return null;
  }

  /**
   * Gets all the transactions where the wallet is the owner
   * @param wallet Wallet address as a string
   * @param count The number of results to return
   * @param cursorId Cursor ID after which to query results, from data.transactions.edges[n].cursor
   * @returns Object with transaction IDs as keys, and transaction data strings as values
   */
  getOwnedTxs(wallet: string, count?: number, cursorId?: string): Promise<any> {
    const countStr = count !== undefined ? `, first: ${count}` : "";
    const afterStr = cursorId !== undefined ? `, after: "${cursorId}"` : "";
    const query = `
      query {
        transactions(owners:["${wallet}"]${countStr}${afterStr}) {
          ${BLOCK_TEMPLATE}
        }
      }`;
    const request = JSON.stringify({ query });
    return this.gql(request);
  }

  /**
   * Gets all the transactions where the wallet is the recipient
   * @param wallet Wallet address as a string
   * @param count The number of results to return
   * @param cursorId Cursor ID after which to query results, from data.transactions.edges[n].cursor
   * @returns Object with transaction IDs as keys, and transaction data strings as values
   */
  getRecipientTxs(
    wallet: string,
    count?: number,
    cursorId?: string
  ): Promise<any> {
    const countStr = count !== undefined ? `, first: ${count}` : "";
    const afterStr = cursorId !== undefined ? `, after: "${cursorId}"` : "";
    const query = `
      query {
        transactions(recipients:["${wallet}"]${countStr}${afterStr}) {
          ${BLOCK_TEMPLATE}
        }
      }`;
    const request = JSON.stringify({ query });
    return this.gql(request);
  }

  /**
   * Get the updated state of an NFT
   * @param contentTxId TxId of the content
   * @param state
   * @returns An object with {totaltViews, totalReward, 24hrsViews}
   */
  async contentView(contentTxId: any, state: any): Promise<any> {
    const rewardReport = state.stateUpdate.trafficLogs.rewardReport;

    try {
      const nftState = await this.readNftState(contentTxId);
      const contentViews = {
        ...nftState,
        totalViews: 0,
        totalReward: 0,
        twentyFourHrViews: 0,
        txIdContent: contentTxId
      };

      rewardReport.forEach((ele: any) => {
        const logSummary = ele.logsSummary;

        for (const txId in logSummary) {
          if (txId == contentTxId) {
            if (rewardReport.indexOf(ele) == rewardReport.length - 1) {
              contentViews.twentyFourHrViews = logSummary[contentTxId];
            }

            const rewardPerAttention = ele.rewardPerAttention;
            contentViews.totalViews += logSummary[contentTxId];
            const rewardPerLog = logSummary[contentTxId] * rewardPerAttention;
            contentViews.totalReward += rewardPerLog;
          }
        }
      });
      return contentViews;
    } catch (err) {
      return null;
    }
  }

  /**
   * Get a list of all NFT IDs
   * @returns Array of transaction IDs which are registered NFTs
   */
  async retrieveAllRegisteredContent(): Promise<string[]> {
    const state = await this.getContractState();
    const registerRecords = state.registeredRecord;
    const txIdArr = Object.keys(registerRecords);
    return txIdArr;
  }

  /**
   * Get a list of NFT IDs by owner
   * @param owner Wallet address of the owner
   * @returns Array containing the NFTs
   */
  async getNftIdsByOwner(owner: string): Promise<string[]> {
    const state = await this.getContractState();
    const nfts = [];
    for (const nft in state.registeredRecord)
      if (state.registeredRecord[nft] === owner) nfts.push(nft);
    return nfts;
  }

  /**
   * Get Koi rewards earned from an NFT
   * @param txId The transaction id to process
   * @returns Koi rewards earned or null if the transaction is not a valid Koi NFT
   */
  async getNftReward(txId: string): Promise<number | null> {
    const state = await this.getContractState();
    if (!(txId in state.registeredRecord)) return null;
    const nft = await this.contentView(txId, state);
    return nft.totalReward;
  }

  /**
   * Query Arweave using GQL
   * @param request Query string
   * @returns Object containing the query results
   */
  async gql(request: string): Promise<any> {
    const { data } = await axios.post(URL_ARWEAVE_GQL, request, {
      headers: { "content-type": "application/json" }
    });
    return data;
  }

  /**
   * Gets an array of service nodes
   * @param url URL of the service node to retrieve the array from a known service node
   * @returns Array of service nodes
   */
  async getNodes(
    url: string = this.bundlerUrl
  ): Promise<Array<BundlerPayload>> {
    const res: any = await getCacheData(url + BUNDLER_NODES);
    try {
      return JSON.parse(res.data);
    } catch (_e) {
      return [];
    }
  }


  /**
   * Gets the list of all KIDs(DIDs)
   * @param count The number of results to return
   * @param cursorId Cursor ID after which to query results, from data.transactions.edges[n].cursor
   * @returns {Array} - returns a Javascript Array of object with each object representing a single KID
   */
  async getAllKID(count?: number, cursorId?: string): Promise<any> {

    const countStr = count !== undefined ? `, first: ${count}` : "";
    const afterStr = cursorId !== undefined ? `, after: "${cursorId}"` : "";
    const query = `
    query {
      transactions(tags: {
        name: "Action",
        values: ["KID/Create"]
    }${countStr}${afterStr}) {
        ${BLOCK_TEMPLATE}
      }
    }`;
    const request = JSON.stringify({ query });
    let gqlResp=await this.gql(request)
    if(gqlResp && gqlResp.data.transactions.edges){
      return gqlResp.data.transactions.edges
    }
    return {message:"No KIDs Found"};
  }

  /**
     * Get the KID state for the particular walletAddress
     * @param walletAddress The wallet address for the person whose DID is to be found
     * @returns {Object} - returns a contract object having id which can be used to get the state 
     */
  async getKIDByWalletAddress(walletAddress?: string): Promise<any> {

    const query = `
      query {
        transactions(tags: [{
          name: "Action",
          values: ["KID/Create"]
      },
        {
          name: "Wallet-Address",
          values: ["${walletAddress}"]
      }
      ]) {
          ${BLOCK_TEMPLATE}
        }
      }`;
    const request = JSON.stringify({ query });
    let gqlResp=await this.gql(request)
    if(gqlResp && gqlResp.data.transactions.edges){
      return gqlResp.data.transactions.edges
    }
    return {message:"No KID Found for this address"};
  }
  /**
   * Creates a KID smartcontract on arweave
   * @param KIDObject - an object containing name, description, addresses and link
   * @param image - an object containing contentType and blobData
   * @returns {txId} - returns a txId in case of success and false in case of failure
   */
  async createKID(KIDObject: any, image: any,): Promise<any> {
    const initialState = KIDObject
    if (initialState && initialState.addresses && initialState.addresses.Arweave) {

      try {
        const tx = await arweave.createTransaction(
          {
            data: image.blobData,
          },
          this.wallet
        );
        tx.addTag('Content-Type', image.contentType);
        tx.addTag('Network', 'Koii');
        tx.addTag('Action', 'KID/Create');
        tx.addTag('App-Name', 'SmartWeaveContract');
        tx.addTag('App-Version', '0.1.0');
        tx.addTag('Contract-Src', 't2jB63nGIWYUTDy2b00JPzSDtx1GQRsmKUeHtvZu1_A');
        tx.addTag('Wallet-Address', initialState.addresses.Arweave);
        tx.addTag('Init-State', JSON.stringify(initialState));
        await arweave.transactions.sign(tx, this.wallet);
        const uploader = await arweave.transactions.getUploader(tx);
        while (!uploader.isComplete) {
          await uploader.uploadChunk();
          console.log(uploader.pctComplete + '% complete', uploader.uploadedChunks + '/' + uploader.totalChunks);
        }
        console.log("TX ID: ", tx.id)
        return tx.id
      } catch (err) {
        console.log('create transaction error');
        console.log('err-transaction', err);
        return false;
      }
    } else {
      console.log('Arweave Address missing in addresses');
      return false;
    }
  }

  /**
   * Updates the state of a KID smartcontract on arweave
   * @param KIDObject - an object containing name, description, addresses and link
   * @param contractId - the contract Id for KID to be updated
   * @returns {txId} - returns a transaction id of arweave for the updateKID smartweave call
   */
  async updateKID(KIDObject: any, contractId: string): Promise<any> {
    const wallet = this.wallet === undefined ? "use_wallet" : this.wallet;

    const txId = await smartweave.interactWrite(arweave, wallet, contractId, {
      function: 'updateKID',
      ...KIDObject
    });
    return txId
  }
  /**
     * Creates a NFT Collection smartcontract on arweave
     * @param collectionObject - an object containing name, description, addresses and link
     * @returns {txId} - returns a txId in case of success and false in case of failure
     */
  async createCollection(collectionObject: any): Promise<any> {
    const initialState = collectionObject
    if(!collectionObject.owner){
      console.log("collectionObject doesn't contain an owner")
      return false
    }
    try {
      const tx = await arweave.createTransaction(
        {
          data: Buffer.from(collectionObject.owner, 'utf8'),
        },
        this.wallet
      );
      tx.addTag('Content-Type', 'text/plain');
      tx.addTag('Network', 'Koii');
      tx.addTag('Action', 'Collection/Create');
      tx.addTag('App-Name', 'SmartWeaveContract');
      tx.addTag('App-Version', '0.1.0');
      tx.addTag('Contract-Src', 'NCepV_8bY831CMHK0LZQAQAVwZyNKLalmC36FlagLQE');
      tx.addTag('Wallet-Address', collectionObject.owner);
      tx.addTag('Init-State', JSON.stringify(initialState));
      await arweave.transactions.sign(tx, this.wallet);
      const uploader = await arweave.transactions.getUploader(tx);
      while (!uploader.isComplete) {
        await uploader.uploadChunk();
        console.log(uploader.pctComplete + '% complete', uploader.uploadedChunks + '/' + uploader.totalChunks);
      }
      console.log("TX ID: ", tx.id)
      return tx.id
    } catch (err) {
      console.log('create transaction error');
      console.log('err-transaction', err);
      return false;
    }
  }
  /**
     * Gets the list of all Collections by walletAddress
     * @param walletAddress The wallet address for the person whose DID is to be found
     * @param count The number of results to return
     * @param cursorId Cursor ID after which to query results, from data.transactions.edges[n].cursor
     * @returns {Array} - returns a Javascript Array of object with each object representing a Collection object (The collection object contains id which can be used in func readState to get actual state)
     */
   async getCollectionsByWalletAddress(walletAddress?: string,count?: number, cursorId?: string): Promise<any> {
    const countStr = count !== undefined ? `, first: ${count}` : "";
    const afterStr = cursorId !== undefined ? `, after: "${cursorId}"` : "";
    const query = `
      query {
        transactions(tags: [{
          name: "Action",
          values: ["Collection/Create"]
      },
        {
          name: "Wallet-Address",
          values: ["${walletAddress}"]
      }
      ]${countStr}${afterStr}) {
          ${BLOCK_TEMPLATE}
        }
      }`;
    const request = JSON.stringify({ query }); 
    let gqlResp=await this.gql(request)
    if(gqlResp && gqlResp.data.transactions.edges){
      return gqlResp.data.transactions.edges
    }
    return {message:"No Collections found for this address"};
  }
  /**
   * Get the state from arweave for any contract
   * @param txId Transaction ID of the NFT
   * @returns The NFT state object
   */
   async readState(txId: string): Promise<any> {
    return smartweave.readContract(arweave, txId);
  }

  /**
   * Add new NFTs to the existing collection
   * @param nftId - The transaction id of the NFT to be added to the collection
   * @param contractId - the contract Id for Collection to be updated
   * @returns {txId} - returns a transaction id of arweave for the updateKID smartweave call
   */
   async addToCollection(nftId: string, contractId: string): Promise<any> {
    const wallet = this.wallet === undefined ? "use_wallet" : this.wallet;

    const txId = await smartweave.interactWrite(arweave, wallet, contractId, {
      function: 'addToCollection',
      nftId
    });
    return txId
  }

  /**
   * Remove NFTs from the existing collection
   * @param index - The index of the NFT which is to be removed from the collection
   * @param contractId - the contract Id for Collection to be updated
   * @returns {txId} - returns a transaction id of arweave for the updateKID smartweave call
   */
   async removeFromCollection(index: number, contractId: string): Promise<any> {
    const wallet = this.wallet === undefined ? "use_wallet" : this.wallet;

    const txId = await smartweave.interactWrite(arweave, wallet, contractId, {
      function: 'removeFromCollection',
      index
    });
    return txId
  }
  /**
   * Updates the view of the existing Collection
   * @param newView - The view you want to set for the collection to display (Initialized with 'default')
   * @param contractId - the contract Id for Collection to be updated
   * @returns {txId} - returns a transaction id of arweave for the updateKID smartweave call
   */
   async updateView(newView: string, contractId: string): Promise<any> {
    const wallet = this.wallet === undefined ? "use_wallet" : this.wallet;

    const txId = await smartweave.interactWrite(arweave, wallet, contractId, {
      function: 'updateView',
      newView
    });
    return txId
  }
  /**
   * Updates the index of the NFT which should be used as the preview for the collection
   * @param imageIndex - The index of the NFT which should be used as the preview for the collection
   * @param contractId - the contract Id for Collection to be updated
   * @returns {txId} - returns a transaction id of arweave for the updateKID smartweave call
   */
   async updatePreviewImageIndex(imageIndex: number, contractId: string): Promise<any> {
    const wallet = this.wallet === undefined ? "use_wallet" : this.wallet;

    const txId = await smartweave.interactWrite(arweave, wallet, contractId, {
      function: 'updatePreviewImageIndex',
      imageIndex
    });
    return txId
  }
  /**
   * Updates the array of NFTs from which the collection is composed of (Can be used to reorder the NFts in the collection also)
   * @param collection - The array of NFTs from which the collection is composed of.
   * @param contractId - the contract Id for Collection to be updated
   * @returns {txId} - returns a transaction id of arweave for the updateKID smartweave call
   */
   async updateCollection(collection: any, contractId: string): Promise<any> {
    const wallet = this.wallet === undefined ? "use_wallet" : this.wallet;

    const txId = await smartweave.interactWrite(arweave, wallet, contractId, {
      function: 'updateCollection',
      collection
    });
    return txId
  }
  /**
   *  Calculates total Views and earned KOII for given NFTIds Array
   * @param nftIdArr - The array of NFTIds for which total Views and earned KOII will be calculated
   * @returns {object} - returns an object containing totalViews and totalRewards
   */
   async getViewsAndEarnedKOII(nftIdArr:any): Promise<any> {
    let state=await this.getContractState()
    if(state){
    const rewardReport = state.stateUpdate?state.stateUpdate.trafficLogs.rewardReport:[];
    let totalViewsOverall=0
    let totalRewardOverall=0
    for(let i=0;i<nftIdArr.length;i++){
      let contentTxId=nftIdArr[i]
      let contentViews = {
        totalViews: 0,
        totalReward: 0,
        twentyFourHrViews: 0
      };
      rewardReport.forEach((ele:any) => {
        let logSummary = ele.logsSummary;

        for (let txId in logSummary) {
          if (txId == contentTxId) {
            if (rewardReport.indexOf(ele) == rewardReport.length - 1) {
              contentViews.twentyFourHrViews = logSummary[contentTxId];
            }

            const rewardPerAttention = ele.rewardPerAttention;
            contentViews.totalViews += logSummary[contentTxId];
            const rewardPerLog = logSummary[contentTxId] * rewardPerAttention;
            contentViews.totalReward += rewardPerLog;
          }
        }
      });
      totalViewsOverall+=contentViews.totalViews
      totalRewardOverall+=contentViews.totalReward
    }
    return {totalViews:totalViewsOverall,totalReward:totalRewardOverall};
    }else{
      return {message:"Views and earned KOII cannot be extracted (State not valid)"}
    }
  }
  // Protected functions

  /**
   * Writes to contract
   * @param input Passes to smartweave write function, in order to execute a contract function
   * @returns Transaction ID
   */
  protected _interactWrite(input: any): Promise<string> {
    const wallet = this.wallet === undefined ? "use_wallet" : this.wallet;

    return smartweave.interactWrite(arweave, wallet, this.contractId, input);
  }

  /**
   * Read contract latest state
   * @returns Contract
   */
  protected async _readContract(): Promise<any> {
    // return smartweave.readContract(arweave, this.contractId);
    const poolID = "OFD4GqQcqp-Y_Iqh8DN_0s3a_68oMvvnekeOEu_a45I";
    const query = new Query(poolID);
    // finding latest transactions
    try {
      const snapshotArray = await query.limit(1).find();
      if (snapshotArray && snapshotArray.length > 0)
        return JSON.parse(snapshotArray[0]).state;
      else console.error("NOTHING RETURNED FROM KYVE");
    } catch (e) {
      console.error("ERROR RETRIEVING FROM KYVE", e);
    }
    return smartweave.readContract(arweave, this.contractId);
  }

  // Private functions

  /**
   * Generate a 12 word mnemonic for an Arweave key https://github.com/acolytec3/arweave-mnemonic-keys
   * @returns {string} - a promise resolving to a 12 word mnemonic seed phrase
   */
  private async _generateMnemonic(): Promise<string> {
    const keys = await generateKeyPair(
      { id: "rsa", modulusLength: 4096 },
      { privateKeyFormat: "pkcs1-pem" }
    );
    return keys.mnemonic;
  }

  /**
   * Generates a JWK object representation of an Arweave key
   * @param mnemonic - a 12 word mnemonic represented as a string
   * @returns {object} - returns a Javascript object that conforms to the JWKInterface required by Arweave-js
   */
  private async _getKeyFromMnemonic(mnemonic: string): Promise<JWKInterface> {
    const keyPair = await getKeyPairFromMnemonic(
      mnemonic,
      { id: "rsa", modulusLength: 4096 },
      { privateKeyFormat: "pkcs1-pem" }
    );

    //@ts-ignore Need to access private attribute
    const privateKey = pem2jwk(keyPair.privateKey);
    delete privateKey.alg;
    delete privateKey.key_ops;
    return privateKey;
  }
}

/**
 * Get cached data from path
 * @param path Path to cached data
 * @returns Data as generic type T
 */
export function getCacheData<T>(path: string): Promise<AxiosResponse<T>> {
  return axios.get(path);
}

/**
 * Get info from Arweave net
 * @returns Axios response with info
 */
function getArweaveNetInfo(): Promise<AxiosResponse<any>> {
  return axios.get(URL_ARWEAVE_INFO);
}

module.exports = {
  BUNDLER_NODES,
  arweave,
  Common,
  getCacheData
};
