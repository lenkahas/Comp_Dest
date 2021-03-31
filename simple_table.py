#!/usr/bin/env python
# coding: utf-8

# In[1]:

import data, numpy
from tqdm import tqdm


def access_slow(flows, step=True, filter_flow=False):
    accessibilities = []
    for row, flowvec in tqdm(flows.iterrows()):
        oid, did = flowvec.origin, flowvec.destination
        if step:
            print(
                f"-----------------------------\nComputing accessibility of {did} to {oid}"
            )
        alternatives = flows.query("origin == @oid")
        if step:
            print(f"\t{oid} can get to {alternatives.destination.tolist()}")
        accessibility = 0
        if oid == did:
            if step:
                print(
                    f"\t\torigin {oid} is exactly destination {did}, so accessibility is zero"
                )
            accessibilities.append(accessibility)
            continue
        for _, alternative in alternatives.iterrows():
            if step:
                input()
            if alternative.destination == did:
                if step:
                    print(
                        f"\t\tSkipping {alternative.destination} because it is target destination {did}"
                    )
                continue
            if alternative.destination == oid:
                if step:
                    print(
                        f"\t\tSkipping {alternative.destination} because it is target origin {oid}"
                    )
                continue
            if filter_flow and (not (alternative.weight > 0)):
                if step:
                    print(
                        f"\t\tSkipping {alternative.destination} because it has negative or zero flow from {oid}"
                    )
                continue
            this_alternative_from_destination = flows.query(
                "origin == @did & destination == @alternative.destination"
            )
            if this_alternative_from_destination.empty:
                if step:
                    print(
                        f"\t\tSkipping {alternative.destination} because there are no routes from {did} to {alternative.destination}"
                    )
                continue
            ak = this_alternative_from_destination.eval(
                "mass_destination * distance"
            ).item()
            if step:
                print(
                    f"\t\t\t A_{oid},{did} gets {this_alternative_from_destination.mass_destination.item()}*{this_alternative_from_destination.distance.item()} from {alternative.destination}"
                )
            accessibility += ak
        accessibilities.append(accessibility)
    return flows.assign(accessibility=accessibilities)


wants_step = input("Do you want to step through the iterations? [Y/n]")
if wants_step.lower().startswith("y"):
    step = True
else:
    step = False


a1 = access_slow(data.toy(), step=step)
print(data.toy())

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
a2 = access_slow(data.toy(), step=step, filter_flow=True)

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

print("-" * 30 + "trying tabular" + "-" * 30)


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


t1 = access(data.toy())
numpy.testing.assert_array_equal(t1.accessibility, a1.accessibility)
print("passed toy tabular unfiltered")

t2 = access(data.toy(), filter_flow=True)
numpy.testing.assert_array_equal(t2.accessibility, a2.accessibility)
print("passed toy tabular filtered")

a3 = access_slow(data.flows(n_hubs=427), step=step, filter_flow=True)
t3 = access(data.flows(n_hubs=427), filter_flow=True)
numpy.testing.assert_array_equal(t3.accessibility, a3.accessibility)
print("passed half tabular filtered")

t4 = access(data.flows(n_hubs=427), filter_flow=False)
a4 = access_slow(data.flows(n_hubs=427), step=step, filter_flow=False)
numpy.testing.assert_array_equal(t4.accessibility, a2.accessibility)
print("passed half tabular unfiltered")

t5 = access(data.flows(), filter_flow=True)
a5 = access_slow(data.flows(), step=step, filter_flow=True)
numpy.testing.assert_array_equal(t5.accessibility, a2.accessibility)
print("passed full tabular filtered")

t6 = access(data.flows(), filter_flow=False)
a6 = access_slow(data.flows(), step=step, filter_flow=False)
numpy.testing.assert_array_equal(t6.accessibility, a2.accessibility)
print("passed full tabular unfiltered")
