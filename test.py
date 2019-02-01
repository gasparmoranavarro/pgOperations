# -*- coding: utf-8 -*-
'''
Created on 1 Feb 2019
@author: Gaspar Mora-Navarro
Polytechnic University of Valencia
joamona@cgf.upv.es
'''

"""
create table d.points (gid serial primary key, description varchar, depth double precision, geom geometry('POINT',25831));
"""



def insert1():
    #Example without dictionaries
    import pgOperations as pgo
    oCon=pgo.pgConnect(database="pruebas", user="postgres", password="postgres", host="localhost", port="5432")

    oStrFielsAndValuesBase=pgo.StrFielsAndValuesBase(
        str_field_names="depth, description, geom", 
        list_field_values=[12.15, "water well","POINT(100 200)"], 
        str_s_values="%s,%s,st_transform(st_geometryfromtext(%s,25830),25831)")
    
    oOp=pgo.pgOperations(oPgConnect=oCon)
    
    resp=oOp.pgInsert(nom_tabla="d.points", oStrFielsAndValues=oStrFielsAndValuesBase, str_fields_returning="gid")
    
    print resp
    
    """
    Result 
    Connected
    Inserting
    insert into d.points (depth, description, geom) values (%s,%s,st_transform(st_geometryfromtext(%s,25830),25831)) returning gid
    [(1,)]
    """
def insert2():
    import pgOperations as pgo

    #Example with dictionaries
    oCon=pgo.pgConnect(database="pruebas", user="postgres", password="postgres", host="localhost", port="5432")

    d={"description": "water well", "depth": 12.15, "geom": "100 200"}
    oStrFielsAndValues=pgo.StrFielsAndValues(d=d, 
                                             list_fields_to_remove=["depth"], 
                                             geom_field_name="geom", 
                                             epsg='25830',
                                             geometry_type="POINT",
                                             epsg_to_reproject="25831")
    
    print oStrFielsAndValues.str_field_names
    print oStrFielsAndValues.list_field_values
    print oStrFielsAndValues.str_s_values
    
    oOp=pgo.pgOperations(oPgConnect=oCon)
    resp=oOp.pgInsert(nom_tabla="d.points", oStrFielsAndValues=oStrFielsAndValues, str_fields_returning="gid")
    print resp
    """
    Result
    
    geom,description
    ['POINT(100 200)', 'water well']
    st_transform(st_geometryfromtext(%s,25830),25831),%s
    Inserting
    insert into d.points (geom,description) values (st_transform(st_geometryfromtext(%s,25830),25831),%s) returning gid
    [(2,)]
    """
def update1():
    import pgOperations as pgo
    oCon=pgo.pgConnect(database="pruebas", user="postgres", password="postgres", host="localhost", port="5432")

    #update the point gid=1 to this new values
    #you can omit the values not to update, or set the parameter list_fields_to_remove
    oStrFielsAndValuesBase=pgo.StrFielsAndValuesBase(
        str_field_names="description, geom", 
        list_field_values=["water well2","POINT(300 300)"], 
        str_s_values="%s,st_transform(st_geometryfromtext(%s,25830),25831)")
        
    print oStrFielsAndValuesBase.str_field_names
    print oStrFielsAndValuesBase.list_field_values
    print oStrFielsAndValuesBase.str_s_values
    
    oOp=pgo.pgOperations(oPgConnect=oCon)
    resp=oOp.pgUpdate(table_name="d.points", 
                      oStrFielsAndValues=oStrFielsAndValuesBase, 
                      cond_where="where gid=%s", 
                      list_values_cond_where=[1])
    print resp    
    
    """
    Result
    
    Connected
    description, geom
    ['water well2', 'POINT(300 300)']
    %s,st_transform(st_geometryfromtext(%s,25830),25831)
    Query: update d.points set (description, geom) = (%s,st_transform(st_geometryfromtext(%s,25830),25831)) where gid=%s
    1
    """
def update2():
    import pgOperations as pgo
    oCon=pgo.pgConnect(database="pruebas", user="postgres", password="postgres", host="localhost", port="5432")

    #update the point gid=1 to this new values
    #you can omit the values not to update, or set the parameter list_fields_to_remove
    d={"description": "water well2", "geom": "300 300"}
    oStrFielsAndValues=pgo.StrFielsAndValues(d=d, 
                                             list_fields_to_remove=None, 
                                             geom_field_name="geom", 
                                             epsg='25830',
                                             geometry_type="POINT",
                                             epsg_to_reproject="25831")
    
    print oStrFielsAndValues.str_field_names
    print oStrFielsAndValues.list_field_values
    print oStrFielsAndValues.str_s_values
    
    oOp=pgo.pgOperations(oPgConnect=oCon)
    resp=oOp.pgUpdate(table_name="d.points", 
                      oStrFielsAndValues=oStrFielsAndValues, 
                      cond_where="where gid=%s", 
                      list_values_cond_where=[1])
    print resp
    """
    Result
    
    geom,description
    ['POINT(300 300)', 'water well2']
    st_transform(st_geometryfromtext(%s,25830),25831),%s
    Query: update d.points set (geom,description) = (st_transform(st_geometryfromtext(%s,25830),25831),%s) where gid=%s
    1

    """
  
def select():
    import pgOperations as pgo
    oCon=pgo.pgConnect(database="pruebas", user="postgres", password="postgres", host="localhost", port="5432")  
    oOp=pgo.pgOperations(oPgConnect=oCon)
    resp=oOp.pgSelect(table_name="d.points", 
                      string_fields_to_select='gid,depth,description,st_astext(geom)', 
                      cond_where = 'where gid > %s', 
                      list_val_cond_where=[0])
    print resp 
    """
    Result
    
    Connected
    SELECT array_to_json(array_agg(registros)) FROM (select gid,depth,description,st_astext(geom) from d.points as t where gid > %s limit 100) as registros;
    [{u'depth': None, u'gid': 2, u'st_astext': u'POINT(-673652.157623927 202.793645345479)', u'description': u'water well'}, 
     {u'depth': 12.15, u'gid': 1, u'st_astext': u'POINT(-673449.363930927 304.189446432685)', u'description': u'water well2'}
    ]

    """

select()   