"""
Script to create and analyze Twitter networks using NetworkX
"""

import argparse
import json
import os
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx


class TwitterGraph:
    BASE_DIR = Path(__file__).parent.parent / "data/networks"

    def __init__(self, location, base_dir=None, network_type=None):
        """
        Initialize the Twitter network graph

        Args:
            location (str): Location/region of the data (e.g., "kolkata")
            base_dir (str, optional): Base directory containing the data.
                If None, uses default.
            network_type (str, optional): Type of network to create.
                If None, uses "retweet".

        Attributes:
            location (str): The location/region of the network data
            base_dir (Path): Base directory containing the network data
            network_type (str): Type of network (e.g., "retweet", "follower")
            graph (nx.DiGraph): NetworkX directed graph object
            num_nodes (int): Number of nodes in the network
            num_edges (int): Number of edges in the network
            avg_degree (float): Average degree of nodes in the network
            pagerank (dict): Dictionary mapping node IDs to their PageRank scores
            top_users (list): List of tuples containing (node_id, score) for top 3 users
        """
        self.location = location
        self.base_dir = base_dir if base_dir is not None else self.BASE_DIR
        self.network_type = (
            network_type if network_type is not None else "retweet"
        )
        self.graph = nx.DiGraph()  # Directed graph for Twitter relationships
        self.num_nodes = 0
        self.num_edges = 0
        self.avg_degree = 0.0
        self.pagerank = {}
        self.top_users = []

    def load_from_json(self):
        """
        Loads graph data from a JSON file containing user interactions.
        Expected JSON format:
        {
            "edges": [
                {
                    "source": "user_id_1",
                    "target": "user_id_2",
                    "source_username": "username1",
                    "target_username": "username2",
                    "source_followers": 1000,
                    "target_followers": 500,
                    "tweet_id": "tweet_id_1",
                },
                ...
            ]
        }
        """
        # Construct the file path
        json_file_path = os.path.join(
            self.base_dir,
            self.location,
            f"{self.network_type}_interactions.json",
        )

        with open(json_file_path, "r") as f:
            data = json.load(f)

        # Clear existing graph
        self.graph.clear()

        # Process edges
        for edge in data["edges"]:
            # Add nodes with attributes if they don't exist
            if edge["source"] not in self.graph:
                self.graph.add_node(
                    edge["source"],
                    username=edge["source_username"],
                    followers=edge["source_followers"],
                )
            if edge["target"] not in self.graph:
                self.graph.add_node(
                    edge["target"],
                    username=edge["target_username"],
                    followers=edge["target_followers"],
                )

            # Add edge with appropriate attributes
            if self.network_type == "retweet":
                if self.graph.has_edge(edge["source"], edge["target"]):
                    # If edge exists, increment weight and append tweet_id
                    self.graph[edge["source"]][edge["target"]]["weight"] += 1
                    self.graph[edge["source"]][edge["target"]][
                        "tweet_ids"
                    ].append(edge["tweet_id"])
                else:
                    # If edge doesn't exist, create it with weight 1
                    self.graph.add_edge(
                        edge["source"],
                        edge["target"],
                        weight=1,
                        tweet_ids=[edge["tweet_id"]],
                    )
            else:  # follower network
                self.graph.add_edge(edge["source"], edge["target"], weight=1)

    def calculate_network_stats(self):
        """
        Calculate and store network statistics.

        This method computes various network metrics and stores them as instance
        attributes:
        - num_nodes: Total number of nodes in the network
        - num_edges: Total number of edges in the network
        - avg_degree: Average degree of nodes in the network
        - pagerank: PageRank scores for all nodes
        - top_users: Top 3 users by PageRank score

        The statistics are used for visualization and analysis purposes.
        """
        self.num_nodes = self.graph.number_of_nodes()
        self.num_edges = self.graph.number_of_edges()
        self.avg_degree = (
            sum(dict(self.graph.degree()).values()) / self.num_nodes
        )
        self.pagerank = nx.pagerank(self.graph, weight="weight")
        self.top_users = sorted(
            self.pagerank.items(), key=lambda x: x[1], reverse=True
        )[:3]

    def visualize_network(self):
        """Creates a visualization of the Twitter network"""
        # Create figure and subplot with proper spacing
        fig, ax = plt.subplots(figsize=(12, 10))
        fig.subplots_adjust(top=0.85)  # Make room for title

        # Calculate network statistics if not already done
        if not self.pagerank:
            self.calculate_network_stats()

        # Format top users text
        top_users_text = "\n".join(
            [
                f"@{self.graph.nodes[user]['username']}: {score:.4f}"
                for user, score in self.top_users
            ]
        )

        # Node size based on degree
        node_sizes = [
            self.graph.degree(node) * 100  # Scale degree for visualization
            for node in self.graph.nodes()
        ]

        # Edge width based on weight
        edge_weights = [
            self.graph[u][v]["weight"] for u, v in self.graph.edges()
        ]
        edge_widths = [
            w * 2 for w in edge_weights
        ]  # Scale weights for visualization

        # Spring layout
        pos = nx.spring_layout(self.graph)

        # Draw the network
        nx.draw(
            self.graph,
            pos,
            ax=ax,
            node_size=node_sizes,
            node_color="lightblue",
            edge_color="gray",
            width=edge_widths,
            with_labels=False,
            alpha=0.7,
        )

        # Add labels using usernames
        labels = {
            node: self.graph.nodes[node]["username"]
            for node in self.graph.nodes()
            if self.graph.degree(node) > 30  # Only label significant nodes
        }
        nx.draw_networkx_labels(self.graph, pos, labels, font_size=8, ax=ax)

        # Add statistics text box
        stats_text = (
            f"Network Statistics:\n"
            f"Nodes: {self.num_nodes}\n"
            f"Edges: {self.num_edges}\n"
            f"Avg Degree: {self.avg_degree:.2f}\n\n"
            f"Top Influential Users:\n{top_users_text}"
        )

        # Add text box with statistics
        ax.text(
            1.05,  # Position on the right side
            0.5,  # Middle of the plot
            stats_text,
            transform=ax.transAxes,
            verticalalignment="center",
            bbox=dict(
                boxstyle="round", facecolor="white", alpha=0.8, edgecolor="gray"
            ),
        )

        # Set title with proper spacing
        title = f"Twitter {self.network_type.title()} Network - {self.location.title()}\n"
        fig.suptitle(title, y=0.95, fontsize=12)

        # Save visualization with high DPI and tight bounding box
        output_dir = os.path.join(
            self.base_dir, self.location, "visualizations"
        )
        os.makedirs(output_dir, exist_ok=True)

        # Save the visualization
        output_path = os.path.join(
            output_dir, f"{self.network_type}_network.png"
        )
        plt.savefig(output_path, bbox_inches="tight", dpi=300, pad_inches=0.5)
        plt.close()  # Close the figure to free memory

    def print_graph_info(self):
        """
        Prints detailed graph information using pre-calculated statistics.

        This method uses the instance attributes that were calculated by
        calculate_network_stats() to display network information.
        """
        # Ensure statistics are calculated
        if not self.pagerank:
            self.calculate_network_stats()

        print(f"\n=== {self.network_type.title()} Network Information ===")
        print(f"Number of nodes: {self.num_nodes}")
        print(f"Number of edges: {self.num_edges}")

        print("\n=== Graph Statistics ===")
        print(f"Graph density: {nx.density(self.graph)}")
        print(f"Average degree: {self.avg_degree:.2f}")

        print("\n=== Centrality Measures ===")
        print("Top 5 users by weighted PageRank:")
        for user_id, score in self.top_users:
            username = self.graph.nodes[user_id]["username"]
            followers = self.graph.nodes[user_id]["followers"]
            print(
                f"@{username} (ID: {user_id}): {score:.4f} "
                f"(Followers: {followers})"
            )


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Create and analyze Twitter networks"
    )
    parser.add_argument(
        "--location",
        type=str,
        required=True,
        help="Location/region of the data (e.g., kolkata)",
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default="",
        help="Base directory containing the data (optional)",
    )
    parser.add_argument(
        "--network-type",
        type=str,
        choices=["retweet", "follower", ""],
        default="",
        help="Type of network to create (optional)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()

    # Convert empty strings to None for optional arguments
    base_dir = args.base_dir if args.base_dir else None
    network_type = args.network_type if args.network_type else None

    # Create and process the network
    print(
        f"\nProcessing {network_type or 'retweet'} network for {args.location}..."
    )
    twitter_graph = TwitterGraph(
        location=args.location, base_dir=base_dir, network_type=network_type
    )

    # Load graph data from JSON
    twitter_graph.load_from_json()

    # Print detailed graph information
    twitter_graph.print_graph_info()

    # Visualize the network
    twitter_graph.visualize_network()
