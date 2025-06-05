from gremlin_python.driver import client, serializer


class NeptuneHandler:
    def __init__(self, endpoint: str, port: int = 8182):
        self.endpoint = f"wss://{endpoint}:{port}/gremlin"
        self.client = None

    def start(self):
        """Initialize the Gremlin client connection."""
        self.client = client.Client(
            self.endpoint,
            "g",
            message_serializer=serializer.GraphSONSerializersV2d0(),
        )

    def stop(self):
        """Close the Gremlin client connection."""
        if self.client:
            self.client.close()

    def run_query(self, query: str, bindings=None):
        """Run a Gremlin query."""
        if not self.client:
            raise RuntimeError("Client not started. Call start() first.")
        try:
            result_set = self.client.submit(query, bindings=bindings)
            result = result_set.all().result()
            return result
        except Exception as e:
            raise RuntimeError(f"Query failed: {e}")

    def user_exists(self, user_id: str) -> bool:
        query = f"g.V('{user_id}').hasLabel('User').limit(1)"
        result = self.run_query(query)
        return len(result) > 0

    def city_exists(self, city_id: str) -> bool:
        query = f"g.V('{city_id}').hasLabel('City').limit(1)"
        result = self.run_query(query)
        return len(result) > 0

    def create_user_node(self, user_dict: dict):
        user_id = user_dict.get("user_id")
        if not user_id:
            raise ValueError("Missing user ID in user_dict")

        # Start Gremlin query with vertex ID and label
        query = f"g.addV('User').property(id, '{user_id}')"

        # Add properties from the dictionary (skip 'id' since it's already used as vertex id)
        for key, value in user_dict.items():
            if key in ["user_id", "description"]:
                continue
            # Handle types
            if isinstance(value, str):
                safe_value = value.replace("'", "\\'")
                query += f".property('{key}', '{safe_value}')"
            elif isinstance(value, (int, float)):
                query += f".property('{key}', {value})"

        # Create city edge if location criteria is met
        if user_dict["city"] == user_dict["target_location"]:
            query += f".as('u').V('{user_dict['city']}').hasLabel('City').as('c').addE('BELONGS_TO').from('u').to('c')"

        # End of query
        query += ".iterate()"

        _ = self.run_query(query)

    def create_follower_edge(self, source_id: str, target_id: str):
        # Check if the FOLLOWS edge already exists
        check_query = f"g.V('{source_id}').outE('FOLLOWS').where(inV().hasId('{target_id}')).limit(1)"
        result = self.run_query(check_query)
        
        if len(result) == 0:
            # Edge doesn't exist; create it
            query = f"""
                    g.V('{source_id}').hasLabel('User').as('a').
                    V('{target_id}').hasLabel('User').as('b').
                    addE('FOLLOWS').from('a').to('b')
                    """
            _ = self.run_query(query)
        else:
            print("FOLLOWS edge already exists")

    def update_node_attributes(
        self, label: str, node_id: str, props_dict: dict
    ):
        query = f"g.V('{node_id}').hasLabel('{label}')"
        for key, value in props_dict.items():
            # Handle types
            if isinstance(value, str):
                safe_value = value.replace("'", "\\'")
                query += f".property(single, '{key}', '{safe_value}')"
            elif isinstance(value, (int, float)):
                query += f".property(single, '{key}', {value})"

        # End of query
        query += ".iterate()"

        _ = self.run_query(query, bindings={"single": "Cardinality.single"})
