#!/usr/bin/env python
# coding: utf-8

# In[1]:

from tqdm import tqdm

def access(flows, step=False, filter_flow=False):
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

if __name__ == "__main__":
    import numpy, data
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
