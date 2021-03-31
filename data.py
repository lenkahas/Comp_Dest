import pandas, geopandas, io
from sklearn import metrics


def flows(n_hubs=None, seed=2478879):
    points = geopandas.read_file("./points.geojson")

    if n_hubs is not None:
        points = points.head(n_hubs)

    distance_matrix = metrics.pairwise_distances(
        points[["X", "Y"]].values, metric="haversine"
    )
    distance_frame = pandas.DataFrame(
        distance_matrix, index=points.ID_code, columns=points.ID_code
    )

    flows = (
        distance_frame.melt(ignore_index=False)
        .rename(columns=dict(ID_code="destination"))
        .reset_index()
        .rename(columns=dict(ID_code="origin", value="distance"))
    )

    points["hubscore"] = numpy.random.exponential(size=points.shape[0])
    points["mass"] = numpy.random.poisson(
        points.hubscore * 100000, size=points.shape[0]
    )

    # each flow is $$f_{ij} \sim poisson\left(\frac{1000h_ih_j}{(100d_{ij})^2 + 100}\right)$$
    # where $h_i$ and $h_j$ are the "hubbiness" of the two places.
    # Each mass $$m_i \sim poisson(h_i*100000)$$

    flows["flow"] = numpy.random.poisson(
        flows.merge(
            points[["ID_code", "hubscore"]], left_on="origin", right_on="ID_code"
        )
        .merge(
            points[["ID_code", "hubscore"]],
            left_on="destination",
            right_on="ID_code",
            suffixes=("_origin", "_destination"),
        )
        .eval("10000*hubscore_origin*hubscore_destination/((100 * distance)**2+100)")
    )

    complete_flows = (
        flows.merge(points[["ID_code", "mass"]], left_on="origin", right_on="ID_code")
        .drop("ID_code", axis=1)
        .merge(
            points[["ID_code", "mass"]],
            left_on="destination",
            right_on="ID_code",
            suffixes=("_origin", "_destination"),
        )
    ).drop("ID_code", axis=1)

    numpy.random.seed(seed)
    return complete_flows.sample(frac=0.9, replace=False)


def toy():
    df = """origin,destination,distance,mass_destination,mass_origin,weight
a,b,2,25,60,0
b,a,2,60,25,5
a,a,0,60,60,80
c,b,20,25,30,10
b,c,20,30,25,7
a,c,10,30,60,2"""
    return pandas.read_csv(io.StringIO(df))
