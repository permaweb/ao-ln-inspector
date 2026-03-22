<p align="center">
  <a href="https://ao.arweave.net">
    <img src="https://arweave.net/AzM59q2tcYzkySUUZUN1HCwfKGVHi--71UdoIk5gPUE"
    height= 500 width=500>
  </a>
</p>

## About

REST API for inspecting AO token (`ao.TN.1`) transfers and messages from the SU, with GQL fallback on missing or delayed notice data.

* assignment blockheight: the Arweave network blockheight where the message was assigned to the SU (first seen).

* settlement blockheight: the Arweave blockheight where the SU-assigned message was settled onchain (uploaded).

Within the same assignment/settlement scope, timestamp references refer to that blockheight's timestamp.

## API

- `GET /v1/token/ao/transfers/{block_id}`
  returns AO token `Transfer` messages **assigned** in that block (read from the SU), with settlement info and related notices when available.
  Notices are scanned from the SU first. if either notice side is missing, the API falls back to GQL notice lookup for that transfer by correlation (`Pushed-For` / transfer id).

- `GET /v1/token/ao/msg/{id}`
  returns the raw SU response for that message id on the AO token process.

- `GET /v1/token/ao/transfer/{id}`
  returns the raw SU transfer response plus any matched `Credit-Notice` and `Debit-Notice` messages.
  this method supports the optional `notice-scan-blocks=x` param to scan extra blocks forward when notices are delayed or assigned into later blocks. default is `1` (2 blocks total: `N` and `N + 1`).
  notices are scanned from the SU first around the transfer assignment window. if anything is missing, the method falls back to GQL retrieval by correlation (`Pushed-For` / transfer id).

## License
This repository is licensed under the [MIT License](./LICENSE)
