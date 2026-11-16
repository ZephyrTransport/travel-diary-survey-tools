"""Graph-based clique detection for joint trip identification.

This module uses NetworkX to build a similarity graph of trips and find
maximal disjoint cliques: groups of mutually matching trips. Where cliques
overlap, the larger one wins.
"""

import logging

import networkx as nx
import polars as pl

logger = logging.getLogger(__name__)


def _build_similarity_graph(filtered_pairs: pl.DataFrame) -> nx.Graph:
    """Build an undirected graph from filtered trip pairs.

    Nodes are linked_trip_ids; an edge connects each pair that passed the
    similarity threshold.

    Args:
        filtered_pairs: DataFrame with ``linked_trip_id`` and ``linked_trip_id_b``.

    Returns:
        NetworkX Graph with trip IDs as nodes and an edge between similar trips
    """
    required_cols = {"linked_trip_id", "linked_trip_id_b"}
    missing = required_cols - set(filtered_pairs.columns)
    if missing:
        msg = f"Missing required columns: {missing}"
        raise ValueError(msg)

    graph = nx.Graph()
    graph.add_edges_from(filtered_pairs.select("linked_trip_id", "linked_trip_id_b").iter_rows())

    logger.debug(
        "Built graph: %d nodes, %d edges",
        graph.number_of_nodes(),
        graph.number_of_edges(),
    )

    return graph


def _no_cliques(all_trip_ids: pl.Series) -> tuple[pl.DataFrame, list[list[int]]]:
    """Every trip with a null clique_id, and no flagged conflicts."""
    return (
        pl.DataFrame({"linked_trip_id": all_trip_ids, "clique_id": [None] * len(all_trip_ids)}),
        [],
    )


def detect_disjoint_cliques(
    filtered_pairs: pl.DataFrame,
    all_trip_ids: pl.Series,
) -> tuple[pl.DataFrame, list[list[int]]]:
    """Detect maximal disjoint cliques, preferring larger ones.

    Finds all maximal cliques. A clique sharing a trip with accepted ones
    replaces them only if it is larger than each (the earlier wins a tie), so
    each trip belongs to at most one joint trip group. Losing cliques are
    returned for review.

    Args:
        filtered_pairs: DataFrame with trip pairs that passed similarity
            threshold. Must contain linked_trip_id and linked_trip_id_b.
        all_trip_ids: Series of all linked_trip_ids to include in output
            (even those not in any joint trip)

    Returns:
        Tuple of:
        - DataFrame with columns:
            - linked_trip_id: Trip identifier
            - clique_id: Sequential clique group ID (null for non-joint trips)
        - List of flagged cliques (those that lost a conflict)
    """
    graph = _build_similarity_graph(filtered_pairs)

    if graph.number_of_nodes() == 0:
        logger.info("No similar trip pairs found, no joint trips detected")
        return _no_cliques(all_trip_ids)

    # Maximal cliques of two or more trips (complete by definition)
    valid_cliques = [c for c in nx.find_cliques(graph) if len(c) >= 2]  # noqa: PLR2004

    logger.info(
        "Found %d maximal cliques from %d trip pairs",
        len(valid_cliques),
        graph.number_of_edges(),
    )

    if len(valid_cliques) == 0:
        return _no_cliques(all_trip_ids)

    # Resolve conflicts: ensure each trip belongs to exactly one clique
    disjoint_cliques: list[list[int]] = []
    node_to_clique: dict[int, list[int]] = {}
    flagged_cliques = []

    for clique in valid_cliques:
        # Every accepted clique sharing a trip with this one, once each.
        rivals = list({id(c): c for n in clique if (c := node_to_clique.get(n))}.values())
        if any(len(clique) <= len(rival) for rival in rivals):
            flagged_cliques.append(clique)
            continue
        for rival in rivals:
            disjoint_cliques.remove(rival)
            for node in rival:
                del node_to_clique[node]
            flagged_cliques.append(rival)
        disjoint_cliques.append(clique)
        for node in clique:
            node_to_clique[node] = clique

    logger.info(
        "Resolved to %d disjoint cliques, flagged %d conflicts",
        len(disjoint_cliques),
        len(flagged_cliques),
    )

    # Build trip -> clique_id mapping (simple sequential IDs)
    trip_to_clique_id: dict[int, int] = {}
    for clique_id, clique in enumerate(disjoint_cliques):
        for trip_id in clique:
            trip_to_clique_id[trip_id] = clique_id

    clique_assignments = pl.DataFrame({"linked_trip_id": all_trip_ids}).with_columns(
        pl.col("linked_trip_id")
        .replace_strict(trip_to_clique_id, default=None, return_dtype=pl.Int64)
        .alias("clique_id")
    )

    num_joint_trips = len(trip_to_clique_id)
    pct_joint = 100 * num_joint_trips / len(all_trip_ids) if len(all_trip_ids) > 0 else 0
    avg_clique_size = num_joint_trips / len(disjoint_cliques) if len(disjoint_cliques) > 0 else 0
    logger.info(
        "%d trips in %d cliques (%.1f%% of all trips, avg size %.1f)",
        num_joint_trips,
        len(disjoint_cliques),
        pct_joint,
        avg_clique_size,
    )

    return clique_assignments, flagged_cliques
