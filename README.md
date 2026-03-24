<p align="center">
  <a href="https://ao.arweave.net">
    <img src="https://arweave.net/AzM59q2tcYzkySUUZUN1HCwfKGVHi--71UdoIk5gPUE"
    height= 500 width=500>
  </a>
</p>

## About

REST API for inspecting canonical AO token (`ao.TN.1`) transfers and messages from the SU.

For notice resolution, the API uses this order:

1. SU notice scan
2. GQL lookup by correlation (`Pushed-For` / transfer id)
3. CU result lookup
4. GQL lookup by `Reference` extracted from CU result

If GQL still cannot hydrate a real notice tx id, the API exposes CU-only notice previews in:

- `pending_credit_notices`
- `pending_debit_notices`

If CU returns a compute failure, the API exposes:

- `compute_error`

* assignment blockheight: the Arweave network blockheight where the message was assigned to the SU (first seen).

* settlement blockheight: the Arweave blockheight where the SU-assigned message was settled onchain (uploaded).

Within the same assignment/settlement scope, timestamp references refer to that blockheight's timestamp.

## API

- `GET /`
  returns API info, configured upstreams, and liveness checks

- `GET /openapi.json`
  returns OpenAPI 3.1 document
  
- `GET /v1/token/ao/transfers/{block_id}`
  returns strict AO token `Transfer` messages **assigned** in that block.
  transfer matching is strict:
  - `Action = Transfer`
  - `Data-Protocol = ao`
  - `Variant = ao.TN.1`
  - `Type = Message`
  - transfer target must be the AO token process
  - assignment owner must be the AO authority
  - assignment `Process` must be the AO token process

  each transfer record may include:
  - `credit_notices`
  - `debit_notices`
  - `pending_credit_notices`
  - `pending_debit_notices`
  - `compute_error`

- `GET /v1/token/ao/msg/{id}`
  returns the raw SU response for that message id on the AO token process.

- `GET /v1/token/ao/transfer/{id}`
  returns one strict AO token transfer plus related notices.
  this method supports the optional `notice-scan-blocks=x` param to scan extra blocks forward on the SU before fallback resolution. default is `1`.

  notice fields follow the same resolution order as above:
  - SU scan
  - GQL by correlation
  - CU result
  - GQL by `Reference`

  if CU reports a compute failure, the response includes `compute_error`.
  if CU can see resulting notices before GQL can hydrate real ids, the response includes `pending_credit_notices` / `pending_debit_notices`.

## License
This repository is licensed under the [MIT License](./LICENSE)
