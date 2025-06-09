import json
import random
import time

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
            if "ConcurrentModificationException" in str(e):
                wait = random.uniform(1.5, 2.5)
                print(
                    f"[RETRY] Conflict detected. Retrying in {wait:.2f} seconds..."
                )
                time.sleep(wait)
            else:
                raise RuntimeError(f"Query failed: {e}")

    def user_exists(self, user_id: str) -> bool:
        query = f"g.V('{user_id}').hasLabel('User').limit(1)"
        result = self.run_query(query)
        return len(result) > 0

    def user_pending_both(self, user_id: str) -> bool:
        # Look for a User vertex whose retweeter_status AND follower_status are both "pending"
        query = (
            f"g.V('{user_id}')"
            f".hasLabel('User')"
            f".has('retweeter_status', 'pending')"
            f".has('follower_status', 'pending')"
            f".limit(1)"
        )
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
            if value is None or (
                isinstance(value, str) and value.strip() == ""
            ):
                safe_value = "null"
            # Handle types
            elif isinstance(value, str):
                # Make string as json compliant - ignore outer quptes
                safe_value = json.dumps(value)[1:-1]
                # additionally escape single quotes for Gremlin
                safe_value = safe_value.replace("'", "\\'")
                query += f".property('{key}', '{safe_value}')"
            elif isinstance(value, (int, float)):
                query += f".property('{key}', {value})"

        # Create city edge if location criteria is met
        if user_dict["city"] == user_dict["target_location"]:
            query += f".as('u').V('{user_dict['city']}').hasLabel('City').as('c').addE('BELONGS_TO').from('u').to('c')"

        # End of query
        query += ".iterate()"

        print(f"\n[DEBUG] Final Gremlin query:\n{query}\n")

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

    def create_retweeter_edge(
        self, source_id: str, target_id: str, tweet_id: str
    ):
        # Check if RETWEETED edge already exists
        check_query = f"""
        g.V('{source_id}').outE('RETWEETED').where(inV().hasId('{target_id}')).
        project('id', 'tweet_ids', 'weight').
            by(id).
            by(values('tweet_ids')).
            by(values('weight'))
        """
        result = self.run_query(check_query)

        if len(result) == 0:
            # Edge doesn't exist; create it
            print("Edge doesn't exist -- Creating it...")
            create_query = f"""
            g.V('{source_id}').hasLabel('User').as('a').
              V('{target_id}').hasLabel('User').as('b').
              addE('RETWEETED').from('a').to('b').
              property('weight', 1).
              property('tweet_ids', '{tweet_id}')
            """
            _ = self.run_query(create_query)
            return

        # Edge exists — extract tweet_ids and weight
        edge_info = result[0]
        print(f"Edge exists with info: {edge_info}")
        existing_tweet_ids = edge_info["tweet_ids"]
        edge_id = edge_info["id"]
        current_weight = int(edge_info["weight"])

        # Convert string of tweet_ids to list
        tweet_id_list = (
            [existing_tweet_ids]
            if ";" not in existing_tweet_ids
            else existing_tweet_ids.split(";")
        )

        if tweet_id in tweet_id_list:
            print("Tweet ID already recorded in RETWEETED edge.")
            return

        # Update edge: increment weight and append tweet_id
        print("Appending new tweet ID info to edge")
        tweet_id_list.append(tweet_id)
        updated_tweet_ids_str = ";".join(tweet_id_list)
        update_query = f"""
            g.E('{edge_id}').
            property('weight', {current_weight + 1}).
            property('tweet_ids', '{updated_tweet_ids_str}')
        """
        _ = self.run_query(update_query)

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

    def extract_node_attribute(
        self, label: str, node_id: str, attribute_name: str
    ):
        """
        Extracts the value of a specific attribute from a node given its label and ID.
        Returns None if the node or attribute does not exist.
        """
        query = (
            f"g.V('{node_id}').hasLabel('{label}').values('{attribute_name}')"
        )
        result = self.run_query(query)
        if not result:
            return None
        elif len(result) == 1:
            return result[0]
        else:
            raise ValueError("Multiple values cannot be returned")
