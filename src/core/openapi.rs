use crate::core::constants::APP_NAME;
use axum::Json;
use serde_json::{Value, json};

pub async fn handle_openapi() -> Json<Value> {
    Json(json!({
        "openapi": "3.1.0",
        "info": {
            "title": APP_NAME,
            "version": env!("CARGO_PKG_VERSION"),
            "description": "AO token inspection API"
        },
        "servers": [
            {
                "url": "/"
            }
        ],
        "paths": {
            "/": {
                "get": {
                    "summary": "Root status",
                    "description": "Returns service info, configured upstreams, and lightweight live dependency checks.",
                    "responses": {
                        "200": {
                            "description": "Service status payload",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/openapi.json": {
                "get": {
                    "summary": "OpenAPI spec",
                    "description": "Returns the OpenAPI specification for this API.",
                    "responses": {
                        "200": {
                            "description": "OpenAPI JSON document",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/token/ao/transfers/{block_id}": {
                "get": {
                    "summary": "AO transfers by assignment block",
                    "description": "Returns strict canonical AO token transfers assigned in the given block. Each transfer includes a status block derived from SU ownership and CU-only execution evidence, and may include settled notices, CU-only pending notices, and a compute_error if CU reported execution failure.",
                    "parameters": [
                        {
                            "name": "block_id",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string"
                            },
                            "description": "AO assignment block height. Transfers are first fetched from the SU inside a padded Arweave timestamp window and then strictly filtered by assignment Block-Height and canonical AO transfer shape."
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Transfer list",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object"
                                    }
                                }
                            }
                        },
                        "400": {
                            "description": "Bad request"
                        },
                        "502": {
                            "description": "Upstream dependency failure"
                        }
                    }
                }
            },
            "/v1/token/ao/msg/{id}": {
                "get": {
                    "summary": "Raw AO message from SU",
                    "description": "Returns the raw SU response for a message id on the AO token process.",
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string"
                            },
                            "description": "AO message id."
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Raw SU message payload",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object"
                                    }
                                }
                            }
                        },
                        "502": {
                            "description": "Upstream dependency failure"
                        }
                    }
                }
            },
            "/v1/token/ao/transfer/{id}": {
                "get": {
                    "summary": "AO transfer with notices",
                    "description": "Returns one strict canonical AO token transfer from SU plus related notices. Notice resolution order is: SU scan, GQL by correlation, CU result, then GQL by Reference. The response also includes a status block derived from SU inclusion and CU result inspection, and may include pending_credit_notices, pending_debit_notices, and compute_error.",
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string"
                            },
                            "description": "AO transfer message id."
                        },
                        {
                            "name": "notice_scan_blocks",
                            "in": "query",
                            "required": false,
                            "schema": {
                                "type": "integer",
                                "minimum": 0
                            },
                            "description": "Extra forward assignment blocks to scan on the SU before fallback resolution. If the requested end block is beyond the current Arweave tip, the SU scan becomes open-ended."
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Transfer with related notices",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object"
                                    }
                                }
                            }
                        },
                        "400": {
                            "description": "Bad request"
                        },
                        "502": {
                            "description": "Upstream dependency failure"
                        }
                    }
                }
            }
        }
    }))
}
