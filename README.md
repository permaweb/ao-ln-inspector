## About

REST API for inspecting AO token (`ao.TN.1`) transfers (and messages) from the SU.

## API

- `GET /v1/token/ao/transfers/{block_id}`
  returns AO token `Transfer` messages **assigned** in that block (reading from the SU), with potential **settlement** info and related notices (if available)

- `GET /v1/token/ao/msg/{id}`
  returns the raw SU response for that message id on the AO token process

- `GET /v1/token/ao/transfer/{id}`
  returns the raw SU transfer response plus any matched `Credit-Notice` and `Debit-Notice` msgs.
  this method support the `notice-scan-blocks=x` optional param to extend the blocks scan magnitude (forward when notices are delayed or assigned into later blocks) for notices. default is 1 (2 blocks scan, N and N + 1)
