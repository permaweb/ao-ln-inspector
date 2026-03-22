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
                    "description": "Returns service info and lightweight live dependency checks.",
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
                    "description": "Returns AO transfer messages assigned in the given block, enriched with settlement metadata and related notices.",
                    "parameters": [
                        {
                            "name": "block_id",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string"
                            },
                            "description": "AO assignment block height."
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
                    "description": "Returns a transfer from SU plus matched credit and debit notices, with GQL fallback for missing notices.",
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
                            "description": "Extra forward assignment blocks to scan on the SU before using GQL fallback."
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
