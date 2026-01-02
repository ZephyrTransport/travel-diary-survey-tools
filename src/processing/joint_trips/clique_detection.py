"""Graph-based clique detection for joint trip identification.

This module uses NetworkX to construct weighted similarity graphs and find
maximal disjoint cliques representing groups of mutually matching trips.
Cliques are ranked by match quality (Mahalanobis distance), temporal overlap,
and size to ensure optimal joint trip assignments.
"""

import itertools
import logging

import networkx as nx
import polars as pl

logger = logging.getLogger(__name__)


def _build_similarity_graph(
    filtered_pairs: pl.DataFrame,
) -> nx.Graph:
    """Build weighted undirected graph from filtered trip pairs.

    Creates graph where nodes are linked_trip_ids and edges connect pairs
    that passed the similarity threshold. Edges are weighted with:
    - mahalanobis: Distance metric (lower is better match)
    - overlap_time: Minutes of temporal overlap (higher is better)

    Args:
        filtered_pairs: DataFrame with columns:
            - linked_trip_id, linked_trip_id_b: Trip pair IDs
            - mahalanobis_distance (optional): Match quality metric
            - temporal_overlap_min (optional): Overlap duration in minutes

    Returns:
        NetworkX Graph with trip IDs as nodes, weighted edges between
        similar trips
    """
    required_cols = {"linked_trip_id", "linked_trip_id_b"}
    missing = required_cols - set(filtered_pairs.columns)
    if missing:
        msg = f"Missing required columns: {missing}"
        raise ValueError(msg)

    graph = nx.Graph()

    # Check for optional weight columns
    has_mahalanobis = "mahalanobis_distance" in filtered_pairs.columns
    has_overlap = "temporal_overlap_min" in filtered_pairs.columns

    # Build edge list with weights
    edge_data = []
    for row in filtered_pairs.iter_rows(named=True):
        u, v = row["linked_trip_id"], row["linked_trip_id_b"]
        mahal = row.get("mahalanobis_distance", 0.0) if has_mahalanobis else 0.0
        overlap = row.get("temporal_overlap_min", 0.0) if has_overlap else 0.0
        edge_data.append((u, v, {"mahalanobis": mahal, "overlap_time": overlap}))

    graph.add_edges_from(edge_data)

    logger.debug(
        "Built graph: %d nodes, %d edges",
        graph.number_of_nodes(),
        graph.number_of_edges(),
    )

    return graph


def _rank_clique_quality(
    clique: list[int],
    graph: nx.Graph,
) -> tuple[float, float, int]:
    """Rank clique by match quality, overlap, and size.

    Returns tuple that can be compared directly (higher is better).
    Ranking criteria in order of priority:
    1. Negative mean Mahalanobis distance (lower distance = better match)
    2. Total temporal overlap (more overlap = better match)
    3. Number of trips in clique (larger groups preferred)

    Args:
        clique: List of node IDs in the clique
        graph: NetworkX Graph with edge weights

    Returns:
        Tuple of (negative_mahalanobis, overlap_sum, clique_size)
        Returns (-inf, 0, 0) if clique has no valid edges
    """
    mahalanobis_sum = 0.0
    overlap_sum = 0.0
    edge_count = 0

    for u, v in itertools.combinations(clique, 2):
        if graph.has_edge(u, v):
            edge_data = graph[u][v]
            mahalanobis_sum += edge_data.get("mahalanobis", 0.0)
            overlap_sum += edge_data.get("overlap_time", 0.0)
            edge_count += 1

    if edge_count == 0:
        return (float("-inf"), 0.0, 0)

    # Expected edges in complete clique
    expected_edges = len(clique) * (len(clique) - 1) // 2
    mean_mahalanobis = mahalanobis_sum / expected_edges

    # Negate mahalanobis so lower distance = higher rank
    return (-mean_mahalanobis, overlap_sum, len(clique))


def _resolve_clique_conflict(
    clique: list[int],
    existing_clique: list[int],
    graph: nx.Graph,
    disjoint_cliques: list[list[int]],
    node_to_clique: dict[int, list[int]],
) -> tuple[list[int], bool]:
    """Resolve conflict between two cliques by quality ranking.

    Args:
        clique: New clique being considered
        existing_clique: Previously assigned clique
        graph: NetworkX Graph with edge weights
        disjoint_cliques: List of accepted cliques (mutated)
        node_to_clique: Mapping of nodes to their cliques (mutated)

    Returns:
        Tuple of (loser clique, is_current_better flag)
    """
    current_quality = _rank_clique_quality(clique, graph)
    existing_quality = _rank_clique_quality(existing_clique, graph)

    is_current_better = current_quality > existing_quality
    loser = existing_clique if is_current_better else clique

    if is_current_better:
        # Replace existing with current
        disjoint_cliques.remove(existing_clique)
        for node in existing_clique:
            del node_to_clique[node]
        disjoint_cliques.append(clique)
        for node in clique:
            node_to_clique[node] = clique

    return loser, is_current_better


def detect_disjoint_cliques(
    filtered_pairs: pl.DataFrame,
    all_trip_ids: pl.Series,
) -> tuple[pl.DataFrame, list[list[int]]]:
    """Detect maximal disjoint cliques with quality-based ranking.

    Finds all maximal cliques and resolves conflicts by ranking cliques
    based on match quality (Mahalanobis distance), temporal overlap, and
    size. Ensures each trip belongs to exactly one joint trip group.

    When trips appear in multiple cliques, the best-quality clique wins.
    Losing cliques have their edges pruned and are flagged for review.

    Args:
        filtered_pairs: DataFrame with trip pairs that passed similarity
            threshold. Must contain linked_trip_id and linked_trip_id_b.
            Optional: mahalanobis_distance, temporal_overlap_min for weights.
        all_trip_ids: Series of all linked_trip_ids to include in output
            (even those not in any joint trip)

    Returns:
        Tuple of:
        - DataFrame with columns:
            - linked_trip_id: Trip identifier
            - joint_trip_id: Assigned group ID (null for non-joint trips)
        - List of flagged cliques (conflicts resolved by pruning)
    """
    # Build weighted similarity graph
    graph = _build_similarity_graph(filtered_pairs)

    if graph.number_of_nodes() == 0:
        logger.info("No similar trip pairs found, no joint trips detected")
        return (
            pl.DataFrame(
                {
                    "linked_trip_id": all_trip_ids,
                    "joint_trip_id": [None] * len(all_trip_ids),
                }
            ),
            [],
        )

    # Find all maximal cliques (guaranteed to be complete by definition)
    all_cliques = list(nx.find_cliques(graph))

    # Filter to cliques of size >= 2
    valid_cliques = [c for c in all_cliques if len(c) >= 2]  # noqa: PLR2004

    logger.info(
        "Found %d maximal cliques from %d trip pairs",
        len(valid_cliques),
        graph.number_of_edges(),
    )

    if len(valid_cliques) == 0:
        return (
            pl.DataFrame(
                {
                    "linked_trip_id": all_trip_ids,
                    "joint_trip_id": [None] * len(all_trip_ids),
                }
            ),
            [],
        )

    # Resolve conflicts: ensure each trip belongs to exactly one clique
    disjoint_cliques = []
    node_to_clique: dict[int, list[int]] = {}  # O(1) lookup for conflicts
    flagged_cliques = []
    edges_to_prune = []

    for clique in valid_cliques:
        # Check for conflicts with existing assignments
        conflict_node = next((n for n in clique if n in node_to_clique), None)

        if conflict_node is None:
            # No conflict - assign this clique
            disjoint_cliques.append(clique)
            for node in clique:
                node_to_clique[node] = clique
            continue

        # Resolve conflict using quality-based ranking
        existing_clique = node_to_clique[conflict_node]
        loser, _ = _resolve_clique_conflict(
            clique, existing_clique, graph, disjoint_cliques, node_to_clique
        )

        # Flag loser and mark edges for pruning
        flagged_cliques.append(loser)
        edges_to_prune.extend(itertools.combinations(loser, 2))

    # Batch prune all losing edges
    if edges_to_prune:
        graph.remove_edges_from(edges_to_prune)

    logger.info(
        "Resolved to %d disjoint cliques, flagged %d conflicts",
        len(disjoint_cliques),
        len(flagged_cliques),
    )

    # Build trip -> joint_trip_id mapping
    trip_to_joint: dict[int, int] = {}
    for joint_id, clique in enumerate(disjoint_cliques, start=1):
        for trip_id in clique:
            trip_to_joint[trip_id] = joint_id

    # Create output DataFrame
    joint_trip_assignments = pl.DataFrame(
        {
            "linked_trip_id": all_trip_ids,
        }
    ).with_columns(
        [
            pl.col("linked_trip_id")
            .map_elements(
                lambda x: trip_to_joint.get(x),
                return_dtype=pl.Int64,
            )
            .alias("joint_trip_id")
        ]
    )

    num_joint_trips = len(trip_to_joint)
    pct_joint = 100 * num_joint_trips / len(all_trip_ids) if len(all_trip_ids) > 0 else 0
    avg_clique_size = num_joint_trips / len(disjoint_cliques) if len(disjoint_cliques) > 0 else 0
    logger.info(
        "%d trips in %d joint groups (%.1f%% of all trips, avg size %.1f)",
        num_joint_trips,
        len(disjoint_cliques),
        pct_joint,
        avg_clique_size,
    )

    return joint_trip_assignments, flagged_cliques
