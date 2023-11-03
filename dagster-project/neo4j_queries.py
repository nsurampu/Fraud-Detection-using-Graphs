class Neo4JQueries:

    def __init__(self):
        self.queries = {}

    def populate_queries(self):

        self.queries['link_shared_identifier'] =  """
            MATCH (c1:Client)-[:HAS_EMAIL|:HAS_PHONE|:HAS_SSN] ->(n)<- [:HAS_EMAIL|:HAS_PHONE|:HAS_SSN]-(c2:Client)
            WHERE id(c1) < id(c2)
            WITH c1, c2, count(*) as cnt
            MERGE (c1) - [:SHARED_IDENTIFIERS {count: cnt}] -> (c2);
        """

        self.queries['create_base_graph'] = """
            CALL gds.graph.project('wcc',
                {
                    Client: {
                        label: 'Client'
                    }
                },
                {
                    SHARED_IDENTIFIERS:{
                        type: 'SHARED_IDENTIFIERS',
                        orientation: 'UNDIRECTED',
                        properties: {
                            count: {
                                property: 'count'
                            }
                        }
                    }
                }
            ) YIELD graphName,nodeCount,relationshipCount,projectMillis;
        """

        self.queries['identify_fraud_rings'] = """
            CALL gds.wcc.stream('wcc',
                {
                    nodeLabels: ['Client'],
                    relationshipTypes: ['SHARED_IDENTIFIERS'],
                    consecutiveIds: true
                }
            )
            YIELD nodeId, componentId
            RETURN gds.util.asNode(nodeId).id AS clientId, componentId
            ORDER BY componentId LIMIT 20
        """

        self.queries['write_fraud_rings_to_db'] = """
            CALL gds.wcc.stream('wcc',
                {
                    nodeLabels: ['Client'],
                    relationshipTypes: ['SHARED_IDENTIFIERS'],
                    consecutiveIds: true
                }
            )
            YIELD componentId, nodeId
            WITH componentId AS cluster, gds.util.asNode(nodeId) AS client
            WITH cluster, collect(client.id) AS clients
            WITH cluster, clients, size(clients) AS clusterSize WHERE clusterSize > 1
            UNWIND clients AS client
            MATCH (c:Client) WHERE c.id = client
            SET c.firstPartyFraudGroup=cluster;
        """

        self.queries['compute_fraud_ring_similarity'] = """
            MATCH(c:Client) WHERE c.firstPartyFraudGroup is not NULL
            WITH collect(c) as clients
            MATCH(n) WHERE n:Email OR n:Phone OR n:SSN
            WITH clients, collect(n) as identifiers
            WITH clients + identifiers as nodes

            MATCH(c:Client) -[:HAS_EMAIL|:HAS_PHONE|:HAS_SSN]->(id)
            WHERE c.firstPartyFraudGroup is not NULL
            WITH nodes, collect({source: c, target: id}) as relationships

            CALL gds.graph.project.cypher('similarity',
                "UNWIND $nodes as n RETURN id(n) AS id,labels(n) AS labels",
                "UNWIND $relationships as r RETURN id(r['source']) AS source, id(r['target']) AS target, 'HAS_IDENTIFIER' as type",
                { parameters: {nodes: nodes, relationships: relationships}}
            )
            YIELD graphName, nodeCount, relationshipCount, projectMillis
            RETURN graphName, nodeCount, relationshipCount, projectMillis
        """

        self.queries['write_fraud_ring_similarity_to_db'] = """
            CALL gds.nodeSimilarity.mutate('similarity',
                {
                    topK:15,
                    mutateProperty: 'jaccardScore',
                    mutateRelationshipType:'SIMILAR_TO'
                }
            );
        """

        self.queries['write_fraud_ring_similarity_to_graph'] = """
            CALL gds.graph.writeRelationship('similarity', 'SIMILAR_TO', 'jaccardScore');
        """

        self.queries['calculate_first_party_fraud_score'] = """
            CALL gds.degree.write('similarity',
                {
                    nodeLabels: ['Client'],
                    relationshipTypes: ['SIMILAR_TO'],
                    relationshipWeightProperty: 'jaccardScore',
                    writeProperty: 'firstPartyFraudScore'
                }
            );
        """

        self.queries['stamp_fraudster'] = """
            MATCH(c:Client)
            WHERE c.firstPartyFraudScore IS NOT NULL
            WITH percentileCont(c.firstPartyFraudScore, 0.9) AS firstPartyFraudThreshold

            MATCH(c:Client)
            WHERE c.firstPartyFraudScore > firstPartyFraudThreshold
            SET c:FirstPartyFraudster;
        """

        self.queries['get_fraudster'] = """
            MATCH(c:Client)
            WHERE c:FirstPartyFraudster
            RETURN c.id AS Client_ID, c.firstPartyFraudScore AS Fraud_Score ORDER BY Fraud_Score DESC
        """

        self.queries['get_fraudster_transactions'] = """
            MATCH p=(c1:Client)-[]-(t:Transaction)-[]-(c2:Client)
            WHERE c1:FirstPartyFraudster
            RETURN c1.id AS Client_ID , c2.id AS Secondary_Client_ID, t.id AS Transaction_ID, t.amount AS Transaction_Amount
        """

        self.queries['add_second_party_tag'] = """
            MATCH (c1:FirstPartyFraudster)-[]->(t:Transaction)-[]->(c2:Client)
            WHERE NOT c2:FirstPartyFraudster
            WITH c1, c2, sum(t.amount) AS totalAmount
            SET c2:SecondPartyFraudSuspect
            CREATE (c1)-[:TRANSFER_TO {amount:totalAmount}]->(c2);
        """

        self.queries['add_transfer_to_relation'] = """
            MATCH (c1:FirstPartyFraudster)<-[]-(t:Transaction)<-[]-(c2:Client)
            WHERE NOT c2:FirstPartyFraudster
            WITH c1, c2, sum(t.amount) AS totalAmount
            SET c2:SecondPartyFraudSuspect
            CREATE (c1)<-[:TRANSFER_TO {amount:totalAmount}]-(c2);
        """

        self.queries['create_transfer_to_projection'] = """
            CALL gds.graph.project('SecondPartyFraudNetwork',
                'Client',
                'TRANSFER_TO',
                {relationshipProperties:'amount'}
            );
        """

        self.queries['create_second_party_clusters'] = """
            CALL gds.wcc.stream('SecondPartyFraudNetwork')
            YIELD nodeId, componentId
            WITH gds.util.asNode(nodeId) AS client, componentId AS clusterId
            WITH clusterId, collect(client.id) AS cluster
            WITH clusterId, size(cluster) AS clusterSize, cluster
            WHERE clusterSize > 1
            UNWIND cluster AS client
            MATCH(c:Client {id:client})
            SET c.secondPartyFraudGroup=clusterId;
        """

        self.queries['second_party_page_rank'] = """
            CALL gds.pageRank.stream('SecondPartyFraudNetwork',
                {relationshipWeightProperty:'amount'}
            )YIELD nodeId, score
            WITH gds.util.asNode(nodeId) AS client, score AS pageRankScore

            WHERE client.secondPartyFraudGroup IS NOT NULL
                    AND pageRankScore > 0 AND NOT client:FirstPartyFraudster

            MATCH(c:Client {id:client.id})
            SET c:SecondPartyFraud
            SET c.secondPartyFraudScore = pageRankScore;
        """

        self.queries['get_mules'] = """
            MATCH p=(:Client:FirstPartyFraudster)-[:TRANSFER_TO]-(c:Client)
            WHERE NOT c:FirstPartyFraudster
            RETURN p;
        """

        self.queries['clear_graphs'] = """
            CALL gds.graph.list()
            YIELD graphName AS namedGraph
            WITH namedGraph
            CALL gds.graph.drop(namedGraph)
            YIELD graphName
            RETURN graphName;
        """
