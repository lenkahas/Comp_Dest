
def access(flow, filter_flow=False):
    wdist_specification = (
        "wdist = distance_d_to_c * mass_destination_d_to_c"
        "* (origin != destination) * (origin != competitor)"
    )
    if filter_flow:
        wdist_specification += " * (weight_o_to_c > 0)"

    return (
        flow.query("origin != destination")
        .groupby("origin")
        # this gives us a set of the "reachable" destinations from origin o,
        # which is also the set of competitors to d
        .destination.agg(set)
        .to_frame("competitor")
        # merge competitors back to each flow, giving the competitors to each destination:
        .merge(flow, left_index=True, right_on="destination", how="right")
        # this duplicates flow o->d for each competitor of destination d
        .explode("competitor")
        # this looks up the flow from origin to competitor and gets their weight.
        .merge(
            flow,
            left_on=("origin", "competitor"),
            right_on=("origin", "destination"),
            suffixes=("", "_o_to_c"),
        )
        # this looks up the flow from destination to competitor and gets their weight
        .merge(
            flow,
            left_on=("destination", "competitor"),
            right_on=("origin", "destination"),
            suffixes=("", "_d_to_c"),
        )
        # this constructs the mass * distance term, but forces it to be zero when:
        # there is no flow from the origin to the competitor
        # the origin is its own competitor
        # the origin is its own destination
        .eval(wdist_specification)
        # now, grouping by flow o -> d lets us compute the sum of wdist,
        # which has already zeroed out competitors with no flow from origin o
        .groupby(["origin", "destination"])
        .wdist.sum()
        # cleaning this up and merging it back into the data frame:
        .reset_index()
        .rename(columns=dict(wdist="accessibility"))
        )

if __name__ == "__main__":
    import data
    a1 = access(data.toy())

    numpy.testing.assert_array_equal(
        a1.accessibility,
        [
            0,  # A_aa is always zero
            30 * 20,  # A_ab is mass of c times distance from b to c
            25 * 20,  # A_ac is mass of b times distance from c to b
            30 * 10,  # A_ba is mass of c times distance from a to c
            0,  # A_bb is always zero
            60 * 10,  # A_bc is mass of a times the distance from c to a
            25 * 2,  # A_ca is mass of b times distance from a to b
            60 * 2,  # A_cb is mass of a times distance from b to a
            0,  # A_cc is always zero
        ],
    )
    print("passed no filter.")
    a2 = access_slow(data.toy(), filter_flow=True)

    numpy.testing.assert_array_equal(
        a2.accessibility,
        [
            0,  # A_aa is always zero
            30 * 20,  # A_ab is mass of c times distance from b to c if flow a -> c
            25 * 20 * 0,  # A_ac is mass of b times distance from c to b if flow a -> b
            30 * 10,  # A_ba is mass of c times distance from a to c if flow b -> c
            0,  # A_bb is always zero
            60 * 10,  # A_bc is mass of a times the distance from c to a if flow b -> a
            25 * 2,  # A_ca is mass of b times distance from c to b if flow c -> b
            60 * 10 * 0,  # A_cb has no routes from c to a if flow c -> a
            0,  # A_cc is always zero
        ],
    )
    print("passed with filter.")

