from pathlib import Path
import datetime
import pandas as pd
import geopandas as gpd
import arcgis
import sqlalchemy


today = datetime.datetime.now().strftime("%Y%m%d")
total_address_fldr = str(Path('../'))
gis = arcgis.gis.GIS(url='https://utah.maps.arcgis.com/')

def counties_etl():
    
    county_df = pd.DataFrame.spatial.from_layer(arcgis.features.FeatureLayer.fromitem(gis.content.get('90431cac2f9f49f4bcf1505419583753')))
    county_df = county_df.reindex(columns=['NAME', 'FIPS_STR', 'SHAPE'])
    county_df = county_df.rename(columns={'NAME':'CountyName', 'FIPS_STR':'County'})
    county_df.spatial.set_geometry("SHAPE")
    county_df.spatial.project(4326)
    county_df.spatial.sr = {"wkid": 4326}

    out_json = Path(total_address_fldr, f'CNTY_49_{today}.json')
    out_json.write_text(county_df.spatial.to_featureset().to_json, encoding='utf-8')

    ## Use if geojson output is preferred
    # out_geojson = Path(total_address_fldr, f'CNTY_49_{today}.geojson')
    # out_geojson.write_text(county_df.spatial.to_featureset().to_geojson, encoding='utf-8') 

#counties_etl()

def municipalities_etl():
    
    muni_df = pd.DataFrame.spatial.from_layer(arcgis.features.FeatureLayer.fromitem(gis.content.get('543fa1f073714198a3dbf8a292bdf30c')))
    muni_df = muni_df.reindex(columns=['NAME', 'SHAPE'])
    muni_df = muni_df.rename(columns={'NAME':'CityName'})
    muni_df.spatial.set_geometry("SHAPE")
    muni_df.spatial.project(4326)
    muni_df.spatial.sr = {"wkid": 4326}
   
    out_json = Path(total_address_fldr, f'CITY_49_{today}.json')
    out_json.write_text(muni_df.spatial.to_featureset().to_json, encoding='utf-8')

    ## Use if geojson output is preferred
    # out_geojson = Path(total_address_fldr, f'CITY_49_{today}.geojson')
    # out_geojson.write_text(muni_df.spatial.to_featureset().to_geojson, encoding='utf-8') 

#municipalities_etl()

def zips_etl():
    
    zips_df = pd.DataFrame.spatial.from_layer(arcgis.features.FeatureLayer.fromitem(gis.content.get('4f9d1d1301864f8cabef08cbae7b2d3c')))
    zips_df = zips_df.reindex(columns=['ZIP5', 'SHAPE'])
    zips_df = zips_df.rename(columns={'ZIP5':'ZipCode'})
    zips_df.spatial.set_geometry("SHAPE")
    zips_df.spatial.project(4326)
    zips_df.spatial.sr = {"wkid": 4326}

    out_json = Path(total_address_fldr, f'ZIPC_49_{today}.json')
    out_json.write_text(zips_df.spatial.to_featureset().to_json, encoding='utf-8')

    ## Use if geojson output is preferred  
    # out_geojson = Path(total_address_fldr, f'ZIPC_49_{today}.geojson')
    # out_geojson.write_text(zips_df.spatial.to_featureset().to_geojson, encoding='utf-8') 

#zips_etl()


def precinct_etl():

    number_to_fips = {
        '0':'49005',
        '1':'49001',
        '2':'49003',
        '3':'49005',
        '4':'49007',
        '5':'49009',
        '6':'49011',
        '7':'49013',
        '8':'49015',
        '9':'49017',
        '10':'49019',
        '11':'49021',
        '12':'49023',
        '13':'49025',
        '14':'49027',
        '15':'49029',
        '16':'49031',
        '17':'49033',
        '18':'49035',
        '19':'49037',
        '20':'49039',
        '21':'49041',
        '22':'49043',
        '23':'49045',
        '24':'49047',
        '25':'49049',
        '26':'49051',
        '27':'49053',
        '28':'49055',
        '29':'49057'
    }

    precinct_df = pd.DataFrame.spatial.from_layer(arcgis.features.FeatureLayer.fromitem(gis.content.get('d33f596419d74948a45070275632b8e0')))
    precinct_df = precinct_df.reindex(columns=['PrecinctID', 'CountyID', 'SHAPE'])
    precinct_df['CountyID'] = precinct_df['CountyID'].map(str)
    precinct_df['CountyID'] = precinct_df['CountyID'].map(number_to_fips)
    precinct_df = precinct_df.rename(columns={'PrecinctID':'PPartName', 'CountyID':'County'})
    precinct_df.spatial.set_geometry("SHAPE")
    precinct_df.spatial.project(4326)
    precinct_df.spatial.sr = {"wkid": 4326}

    out_json = Path(total_address_fldr, f'PRCT_49_{today}.json')
    out_json.write_text(precinct_df.spatial.to_featureset().to_json, encoding='utf-8')

    ## Use if geojson output is preferred
    # out_geojson = Path(total_address_fldr, f'PRCT_49_{today}.geojson')
    # out_geojson.write_text(precinct_df.spatial.to_featureset().to_geojson, encoding='utf-8') 

#precinct_etl()


def address_etl():
    print('start address point etl')
    engine = sqlalchemy.create_engine(
                sqlalchemy.engine.url.URL.create(
                    drivername="postgresql",
                    username='agrc',
                    password='agrc',
                    database='opensgid',
                    host='opensgid.agrc.utah.gov',
                    port=5432,
                )
            )

    addr_type_map = {
        'BASE ADDRESS':'',
        'Commercial':'C',
        'Industrial':'C',
        'Other':'',
        'Residential':'',
        'Unknown':'',
        '':''
        }
    
    field_map = {
        'fulladd':'Original Addr string',
        'addnum':'Addr #',
        'prefixdir':'Prefix',
        'streetname':'Street Name',
        'streettype':'Street type',
        'suffixdir':'Suffix',
        'unitid':'Multi-Unit',
        'city':'City',
        'zipcode':'Zip code',
        'countyid':'County',
        'pttype':'Addr Type'
    }
    
    add_pts_table = 'opensgid.location.address_points'
    add_df = gpd.read_postgis(
        f"select * from {add_pts_table}", engine, index_col='xid', crs=26912, geom_col='shape'
    )

    add_df.to_crs(crs=4326, epsg=None, inplace=True)

    add_sdf = pd.DataFrame.spatial.from_geodataframe(add_df, column_name='shape')
   
    add_sdf = add_sdf.reindex(columns=['fulladd', 'addnum', 'addnumsuffix', 'prefixdir', 'streetname', 'streettype',
                                       'suffixdir', 'unitid', 'city', 'zipcode', 'countyid', 'pttype', 'shape'])
    
    add_sdf.spatial.set_geometry('shape')
 
    add_sdf['addnum'] = add_sdf['addnum'].astype(str)
    add_sdf['addnum'] = add_sdf[['addnum', 'addnumsuffix']].agg(''.join, axis=1)

    add_sdf['Lat'] = add_sdf['shape'].apply(lambda shape: shape.y)
    add_sdf['Lon'] = add_sdf['shape'].apply(lambda shape: shape.x)
    
    add_sdf = add_sdf.drop('shape', axis=1)
    add_sdf['unitid'] = add_sdf['unitid'].apply(lambda x: '1' if x != '' else '0')
    add_sdf['pttype'] = add_sdf['pttype'].map(addr_type_map)
    add_sdf = add_sdf.rename(columns=field_map)
    add_sdf = add_sdf.reindex(columns=['Original Addr string', 'Addr #', 'Prefix', 'Street Name','Street type', 'Suffix', 'Multi-Unit',
                                       'City', 'Zip code', 'plus 4', 'County', 'Addr Type', 'Precinct Part Name', 'Lat', 'Lon'])
    
    address_csv = Path(total_address_fldr, f'ADDRE_49_{today}.csv')
    add_sdf.to_csv(address_csv)


address_etl()