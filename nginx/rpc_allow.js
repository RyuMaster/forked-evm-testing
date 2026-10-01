// JSON-RPC method allowlist for requests that arrive through the Cloudflare tunnel (native spec
// 2026-10-01-play-internal-test §3.2). The lists are every method the app sends (net/GspClient.kt,
// engine/net/GspPoller.cpp + FeedClient.cpp, net/ChainReads.kt, ChainClient.kt, LiveChain.kt,
// HelperClient.kt). LAN and in-stack callers (xayax, helper, fanout) never come through here.
// Anything else -- a batch with one stranger in it, garbage, an empty batch -- is answered here with a
// JSON-RPC error and never forwarded.
const GSP = ['getnullstate', 'getaccounts', 'getcharacters', 'getserviceinfo', 'getbuildings',
             'getongoings', 'getbootstrapdata', 'getregions', 'getgroundloot'];
const CHAIN_READ = ['eth_call', 'eth_getTransactionReceipt', 'eth_blockNumber', 'eth_chainId'];
const CHAIN_TEST = CHAIN_READ.concat(['eth_sendTransaction', 'anvil_setStorageAt',
                                      'anvil_impersonateAccount', 'evm_mine']);
const CHAIN_LIVE = CHAIN_READ.concat(['eth_getBalance', 'eth_getTransactionCount', 'eth_gasPrice',
                                      'eth_sendRawTransaction', 'eth_getTransactionByHash']);
const HELPER = ['getname', 'sendmove', 'syncgsp', 'transfertoken'];
const MAX_BATCH = 20;

function refusal(text, allowed) {
    let req;
    try { req = JSON.parse(text); } catch (e) { return { id: null, code: -32700, message: 'parse error' }; }
    const calls = Array.isArray(req) ? req : [req];
    if (calls.length === 0 || calls.length > MAX_BATCH) {
        return { id: null, code: -32600, message: 'invalid request' };
    }
    for (let i = 0; i < calls.length; i++) {
        const c = calls[i];
        if (c === null || typeof c !== 'object' || typeof c.method !== 'string') {
            return { id: null, code: -32600, message: 'invalid request' };
        }
        if (allowed.indexOf(c.method) < 0) {
            return { id: c.id === undefined ? null : c.id, code: -32601, message: 'method not allowed' };
        }
    }
    return null;
}

async function gate(r, upstream, allowed) {
    const text = r.requestText === undefined ? '' : r.requestText;
    const no = refusal(text, allowed);
    r.headersOut['Content-Type'] = 'application/json';
    if (no) {
        r.return(200, JSON.stringify({ jsonrpc: '2.0', id: no.id,
                                       error: { code: no.code, message: no.message } }));
        return;
    }
    const reply = await r.subrequest(upstream, { method: 'POST', body: text });
    r.return(reply.status, reply.responseText);
}

export default {
    gsp:       (r) => gate(r, '/_up/gsp', GSP),
    chainTest: (r) => gate(r, '/_up/chain', CHAIN_TEST),
    chainLive: (r) => gate(r, '/_up/chain', CHAIN_LIVE),
    helper:    (r) => gate(r, '/_up/helper', HELPER),
};
