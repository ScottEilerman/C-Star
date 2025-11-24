import re
from pathlib import Path
from typing import Literal, cast

import networkx as nx
from matplotlib import pyplot as plt

from cstar.orchestration.models import Workplan
from cstar.orchestration.orchestration import Planner


def slugify(source: str) -> str:
    """Convert a source string into a URL-safe slug.

    Parameters
    ----------
    source : str
        The string to be converted.

    Returns
    -------
    str
        The slugified version of the source string.
    """
    if not source:
        raise ValueError

    return re.sub(r"\s+", "-", source.casefold())


START_NODE: Literal["_cs_start_"] = "_cs_start_"
TERMINAL_NODE: Literal["_cs_term_"] = "_cs_term_"


def _add_marker_nodes(graph: nx.DiGraph) -> nx.DiGraph:
    """Add node to serve as the entrypoint and exit point of the task graph.
    Parameters
    ----------
    graph : nx.DiGraph
        The source graph.
    Returns
    -------
    nx.DiGraph
        A copy of the original graph with the entrypoint node inserted
    """
    graph = cast("nx.DiGraph", graph.copy())
    nx.set_node_attributes(graph, "task", "action")

    if START_NODE not in graph.nodes:
        graph.add_node(
            START_NODE,
            **{"action": "start"},
        )
    else:
        graph.nodes[START_NODE]["action"] = "start"

    if TERMINAL_NODE not in graph.nodes:
        graph.add_node(
            TERMINAL_NODE,
            **{"action": "term"},
        )
    else:
        graph.nodes[TERMINAL_NODE]["action"] = "term"

    # find steps with no dependencies, allowing immediate start
    no_dep_edges = [
        (START_NODE, node)
        for node in graph.nodes()
        if graph.in_degree(node) == 0 and node not in [START_NODE, TERMINAL_NODE]
    ]

    # Add edges from the  start node to all independent steps
    graph.add_edges_from(no_dep_edges)

    # find steps that have no tasks after them
    terminal_edges = [
        (node, TERMINAL_NODE)
        for node in graph.nodes()
        if graph.out_degree(node) == 0 and node != TERMINAL_NODE
    ]

    # Add edges from leaf nodes to the terminal node
    graph.add_edges_from(terminal_edges)

    return graph


def _create_color_map(
    start_color: str = "#00ff00a1",
    term_color: str = "#ff7300a1",
    task_color: str = "#377aaaa1",
) -> dict[str, str]:
    """Return a dictionary mapping node types to color.
    Parameters
    ----------
    start_color : str
        Color of the start node
    term_color : str
        Color of the terminal node
    task_color : str
        Color of the task nodes
    Returns
    -------
    dict[str, str]
    """
    return {
        "start": start_color,
        "term": term_color,
        "task": task_color,
    }


def _get_color_map(
    group_map: dict[str, str],
    graph: nx.DiGraph,
) -> tuple[str, ...]:
    """Return a pyplot-usable iterable containing per-node coloring.
    Paramters
    ---------
    group_map : dict[str, str]
        A mapping of node behavior names to colors
    graph : nx.DiGraph
        The graph to create a color-map for
    Returns
    -------
    tuple[str, ...]
        tuple containing color strings
    """
    return tuple(
        group_map[node_data["action"]] for node, node_data in graph.nodes(data=True)
    )


def render(
    planner: Planner,
    image_directory: Path,
    layout: str = "circular",
    cmap: str = "",
) -> Path:
    """Render the graph to a file.

    Parameters
    ----------
    planner : Planner
        The graph to render
    image_directory : Path
        The directory to render the file to
    layout : str
        The graph layout to apply
    cmap : str
        A color map to apply to nodes

    Returns
    -------
    Path
        The path to the output file
    """
    plt.figure(figsize=(11, 8))
    plt.cla()
    plt.clf()

    graph = _add_marker_nodes(planner.graph)
    name_map = {v: k for k, v in planner._tasks.items()}
    name_map.update({START_NODE: "start", TERMINAL_NODE: "end"})
    color_map = _create_color_map()

    if START_NODE not in graph:
        msg = "Start node was not found. Graph will not be rendered."
        print(msg)
        return Path("not-found")

    if layout == "spring":
        pos = nx.spring_layout(graph)
    elif layout == "circular":
        pos = nx.circular_layout(graph)
    elif layout == "kamada":
        pos = nx.kamada_kawai_layout(graph)
    elif layout == "shell":
        pos = nx.shell_layout(graph)
    elif layout == "spiral":
        pos = nx.spiral_layout(graph)
    elif layout == "planar":
        pos = nx.planar_layout(graph)
    elif layout == "fruchterman":
        pos = nx.fruchterman_reingold_layout(graph)
    else:
        # WARNING: bfs_layout appears to require nx >= 3.5
        pos = nx.bfs_layout(graph, START_NODE)

    node_colors = _get_color_map(color_map, graph)
    nx.draw_networkx(
        graph,
        pos,
        with_labels=True,
        labels=name_map,
        node_size=2000,
        node_color=range(len(graph.nodes)) if cmap else node_colors,
        font_weight="bold",
        cmap=cmap if cmap else None,
    )

    plt.tight_layout(pad=2.0)

    write_to = image_directory / f"{slugify(planner.workplan.name).lower()}.png"
    plt.savefig(write_to, bbox_inches="tight", dpi=500)  # was "tight"

    return write_to
