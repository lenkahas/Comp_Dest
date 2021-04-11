
if __name__ == "__main__":
    import data
    from access import access
    from access_jit import access as access_jit
    from access_tab import access as access_tab
    from datetime import datetime
    print("-" * 30 + "confirm agreement" + "-" * 30)

    filter_flows = False
    a1 = access(data.toy(), filter_flow=filter_flows)
    j1 = access_jit(data.toy(), filter_flow=filter_flows)
    t1 = access_tab(data.toy(), filter_flow=filter_flows)

    numpy.testing.assert_array_equal(t1.accessibility, a1.accessibility)
    numpy.testing.assert_array_equal(t1.accessibility, j1.accessibility)
    print("passed toy tabular unfiltered")

    filter_flows = True
    a1f = access(data.toy(), filter_flow=filter_flows)
    j1f = access_jit(data.toy(), filter_flow=filter_flows)
    t1f = access_tab(data.toy(), filter_flow=filter_flows)

    numpy.testing.assert_array_equal(t1f.accessibility, a1f.accessibility)
    numpy.testing.assert_array_equal(t1f.accessibility, j1f.accessibility)
    print("passed toy tabular unfiltered")

    filter_flows = False
    a2 = access(data.flows(n_hubs=20), filter_flow=filter_flows)
    j2 = access_jit(data.flows(n_hubs=20), filter_flow=filter_flows)
    t2 = access_tab(data.flows(n_hubs=20), filter_flow=filter_flows)
    
    numpy.testing.assert_array_equal(t2.accessibility, a2.accessibility)
    numpy.testing.assert_array_equal(t2.accessibility, j2.accessibility)
    print("passed f20 tabular unfiltered")

    filter_flows = True
    a2f = access(data.flows(n_hubs=20), filter_flow=filter_flows)
    j2f = access_jit(data.flows(n_hubs=20), filter_flow=filter_flows)
    t2f = access_tab(data.flows(n_hubs=20), filter_flow=filter_flows)
    
    numpy.testing.assert_array_equal(t2f.accessibility, a2f.accessibility)
    numpy.testing.assert_array_equal(t2f.accessibility, j2f.accessibility)
    print("passed f20 tabular unfiltered")

    filter_flows = False
    a3 = access(data.flows(n_hubs=400), filter_flow=filter_flows)
    j3 = access_jit(data.flows(n_hubs=400), filter_flow=filter_flows)
    t3 = access_tab(data.flows(n_hubs=400), filter_flow=filter_flows)
    
    numpy.testing.assert_array_equal(t3.accessibility, a3.accessibility)
    numpy.testing.assert_array_equal(t3.accessibility, j3.accessibility)
    print("passed f400 tabular unfiltered")

    filter_flows = True
    a3f = access(data.flows(n_hubs=400), filter_flow=filter_flows)
    j3f = access_jit(data.flows(n_hubs=400), filter_flow=filter_flows)
    t3f = access_tab(data.flows(n_hubs=400), filter_flow=filter_flows)
    
    numpy.testing.assert_array_equal(t3f.accessibility, a3f.accessibility)
    numpy.testing.assert_array_equal(t3f.accessibility, j3f.accessibility)
    print("passed f400 tabular unfiltered")

    filter_flows = False
    a4 = access(data.flows(n_hubs=None), filter_flow=filter_flows)
    j4 = access_jit(data.flows(n_hubs=None), filter_flow=filter_flows)
    t4 = access_tab(data.flows(n_hubs=None), filter_flow=filter_flows)
    
    numpy.testing.assert_array_equal(t4.accessibility, a4.accessibility)
    numpy.testing.assert_array_equal(t4.accessibility, j4.accessibility)
    print("passed f tabular unfiltered")

    filter_flows = True
    a4f = access(data.flows(n_hubs=None), filter_flow=filter_flows)
    j4f = access_jit(data.flows(n_hubs=None), filter_flow=filter_flows)
    t4f = access_tab(data.flows(n_hubs=None), filter_flow=filter_flows)
    
    numpy.testing.assert_array_equal(t4f.accessibility, a4f.accessibility)
    numpy.testing.assert_array_equal(t4f.accessibility, j4f.accessibility)
    print("passed f tabular unfiltered")
