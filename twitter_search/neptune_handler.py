from gremlin_python.driver import client, serializer
from config_utils.constants import NEPTUNE_ENDPOINT

class NeptuneHandler:
    def __init__(self, endpoint: str, port: int = 8182):
        self.endpoint = f"wss://{endpoint}:{port}/gremlin"
        self.client = None

    def start(self):
        """Initialize the Gremlin client connection."""
        self.client = client.Client(
            self.endpoint,
            'g',
            message_serializer=serializer.GraphSONSerializersV2d0()
        )

    def stop(self):
        """Close the Gremlin client connection."""
        if self.client:
            self.client.close()

    def run_query(self, query: str):
        """Run a Gremlin query."""
        if not self.client:
            raise RuntimeError("Client not started. Call start() first.")
        try:
            result_set = self.client.submit(query)
            print(result_set)
            result = result_set.all().result()
            return result
        except Exception as e:
            print(f"Query failed: {e}")
            return {"error": str(e)}

    def create_vertex(self, label: str, properties: dict):
        """Create a vertex with label and properties."""
        prop_str = ''.join([f".property('{k}', '{v}')" for k, v in properties.items()])
        query = f"g.addV('{label}'){prop_str}"
        return self.run_query(query)

    def get_vertices_by_label(self, label: str, limit: int = 5):
        """Retrieve vertices with the given label."""
        query = f"g.V().hasLabel('{label}').limit({limit})"
        return self.run_query(query)
    
    def get_node_attributes_by_label(self, label: str, limit: int = 10):
        """
        Retrieve all attributes (properties) of nodes with the given label.

        Returns a list of dictionaries, each representing a node's properties.
        """
        query = f"g.V().hasLabel('{label}').limit({limit}).valueMap(true)"
        return self.run_query(query)

    def delete_vertices_by_label(self, label: str):
        """Delete all vertices with the specified label."""
        query = f"g.V().hasLabel('{label}').drop()"
        return self.run_query(query)
    

if __name__ == "__main__":
    handler = NeptuneHandler(NEPTUNE_ENDPOINT)
    handler.start()

    # # Test 1: Create vertex
    # print("=== Create Vertex ===")
    # result = handler.create_vertex("Person", {"name": "Alice", "role": "Analyst"})
    # print(result)

    # Test 2: Read vertex
    print("=== Read Vertex ===")
    result = handler.get_node_attributes_by_label("Person", limit=3)
    print(result)

    # # Test 3: Delete vertex
    # print("=== Delete Vertex ===")
    # result = handler.delete_vertices_by_label("Person")
    # print(result)

    handler.stop()
