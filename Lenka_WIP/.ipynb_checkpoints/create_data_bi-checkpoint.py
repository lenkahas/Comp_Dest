import pandas, geopandas, numpy, itertools
from geopy.distance import great_circle


def flows():
    points = geopandas.read_file('./points.geojson')
    flow = pandas.read_csv('./sample_prescription_data.csv')
    
    # distance matrix
    all_flows = pd.DataFrame( list( itertools.product(list(points['Place_ID'].unique()),list(points['Place_ID'].unique())))
                       ).rename(columns = {0:'Origin_ID',
                                        1:'Dest_ID'})
    
    # create stripped column of coordinates 
    points['xy'] = points.geometry.apply(lambda x: [x.y, x.x])
    points = pd.DataFrame(points.loc[:,['Place_ID','xy']])
    
    all_flows = all_flows.merge(points, how = 'left', left_on = 'Origin_ID', right_on = 'Place_ID').rename(columns = {'xy' : 'xy_O'})
    all_flows = all_flows.merge(points, how = 'left', left_on = 'Dest_ID', right_on = 'Place_ID').rename(columns = {'xy' : 'xy_D'})
    all_flows['distance'] = all_flows.apply(lambda x: great_circle(x.xy_O, x.xy_D).km, axis=1)
    
    # Join the flows
    flow = flow.loc[:,['Origin_ID','Dest_ID','Weight','Dest_mass']].groupby(['Origin_ID','Dest_ID']).mean().reset_index()
    all_flows = all_flows.loc[:,['Origin_ID','Dest_ID', 'distance']].merge(flow.loc[:,['Origin_ID','Dest_ID','Weight']], how = 'left', on = ['Origin_ID','Dest_ID']
                                                              ).merge(flow.loc[:,['Dest_ID','Dest_mass']], how = 'left', on = 'Dest_ID')  
    
