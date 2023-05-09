from config import ArangoDBConfig


class KnowledgeGraphModel:
    edgeDefinitions = [
        {
            'edge_collection': ArangoDBConfig.TRANSFERS,
            'from_vertex_collections': [ArangoDBConfig.WALLETS],
            'to_vertex_collections': [ArangoDBConfig.WALLETS]
        },
        {
            'edge_collection': ArangoDBConfig.DEPOSITS,
            'from_vertex_collections': [ArangoDBConfig.WALLETS],
            'to_vertex_collections': [ArangoDBConfig.POOLS]
        },
        {
            'edge_collection': ArangoDBConfig.BORROWS,
            'from_vertex_collections': [ArangoDBConfig.WALLETS],
            'to_vertex_collections': [ArangoDBConfig.POOLS]
        },
        {
            'edge_collection': ArangoDBConfig.WITHDRAWS,
            'from_vertex_collections': [ArangoDBConfig.WALLETS],
            'to_vertex_collections': [ArangoDBConfig.POOLS]
        },
        {
            'edge_collection': ArangoDBConfig.REPAYS,
            'from_vertex_collections': [ArangoDBConfig.WALLETS],
            'to_vertex_collections': [ArangoDBConfig.POOLS]
        },
        {
            'edge_collection': ArangoDBConfig.LIQUIDATES,
            'from_vertex_collections': [ArangoDBConfig.WALLETS],
            'to_vertex_collections': [ArangoDBConfig.POOLS]
        }
    ]
    _orphanedCollections = []
