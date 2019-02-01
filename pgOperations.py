# -*- coding: utf-8 -*-
'''
Created on 16 feb. 2018
@author: Gaspar Mora-Navarro
Polytechnique University of Valencia
Department of Cartographic Engineering Geodesy and Photogrammetry
Higher Technical School of Geodetic, Cartographic and Topographical Engineering
joamona@cgf.upv.es

This library allow you to perform the most common operations with PostGIS:
    - Connection, insert, delete, update, select, create and drop databases

You first have to get a object of the classs pgConnect. Later you can create
and object pgOperations to perform the operations. The pgOperations methods are
applicable to any database and any table, with or without any type of geometry

This library depends of the psycopg2 library (http://initd.org/psycopg/)
This library uses Python 2.7

In the method examples it is assumed that you have created the following table:
create table d.points (gid serial primary key, description varchar, depth double precision, geom geometry('POINT',25831));

'''

import psycopg2
import psycopg2.extensions
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

import json

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

class pgConnect():
    """Connects with the database"""
    conn=None
    """psycopg2 connection object - class variable"""
    cursor=None
    """psycopg2 connection object - class variable"""
    
    def __init__(self, database,user,password,host,port):
        """Initialise the class variables"""
        d=self.__connect(database, user, password, host, port)
        self.conn=d['conn']
        self.cursor=d['cursor']
        print 'Connected'
        
    def __connect(self,database,user,password,host,port):
        """
        Connects with the database with the library psycopg2
        The credentials of the connection are imported from the file var_globales.py
        @return a dictionary wirh the connection and the cursor of the connection
            {'conn':conn, 'cursor':cursor}
        """
        #conexion
        conn=psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        cursor=conn.cursor()
        d={}
        d['conn']=conn
        d['cursor']=cursor
        return d
    
    def disconnect(self):
        """Closes the connection"""
        self.cursor.close()
        self.conn.close()
        print 'Disconected'


class pgOperations():
    """
    Perform the most common operations with PostGIS:
    - Connection, insert, delete, update, select
    """
    query=None
    """Store the last query - class variable"""
    oPgConnect=None
    """pgConnection object - class variable"""
    def __init__(self, oPgConnect):
        self.oPgConnect=oPgConnect
        
    def pgInsert(self, nom_tabla, oStrFielsAndValues, str_fields_returning=None):
        """
        Inserts a row in a table
        
        @param nom_tabla: table name included the schema. Ej. "d.linde". 
            Mandatory specify the schema name: public.tablename
        @type  nom_tabla: string
        @param oStrFielsAndValues: object with the field names and values to insert
        @type oStrFielsAndValues: StrFielsAndValues
        @param str_fields_returning: string with the field names, of the inserted  row, to return.
            ej: "gid, date", will return the field gid and date
        @type str_fields_returning: string

        @return:
            if str_fields_returning is None, returns None
            if str_fields_returning is 'gid, date' returns a list with a tuple with the gid and date
                gid and date have to be fields of the table.
                This is used to know the gid of the new row inserted
        Example
        1. Example of use without dictionaries:
        
        import pgOperations2 as pgo
        oCon=pgo.pgConnect(database="pruebas", user="postgres", password="postgres", host="localhost", port="5432")
        oStrFielsAndValuesBase=pgo.StrFielsAndValuesBase(
            str_field_names="depth, description, geom", 
            list_field_values=[12.15, "water well","POINT(100 200)"], 
            str_s_values="%s,%s,st_transform(st_geometryfromtext(%s,25830),25831)")
        oOp=pgo.pgOperations(oPgConnect=oCon)
        resp=oOp.pgInsert(nom_tabla="d.points", oStrFielsAndValues=oStrFielsAndValuesBase, str_fields_returning="gid")
        print resp
        
        The result is:
        Connected
        Inserting
        insert into d.points (depth, description, geom) values (%s,%s,st_transform(st_geometryfromtext(%s,25830),25831)) returning gid
        [(1,)]
        
        2. Example of use with dictionaries, and automatic generation of expressions
        
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
        
        resp=oOp.pgInsert(nom_tabla="d.points", oStrFielsAndValues=oStrFielsAndValues, str_fields_returning="gid")
        print resp
        
        The result is
        geom,description
        ['POINT(100 200)', 'water well']
        st_transform(st_geometryfromtext(%s,25830),25831),%s
        Inserting
        insert into d.points (geom,description) values (st_transform(st_geometryfromtext(%s,25830),25831),%s) returning gid
        [(2,)]
        """
        conn=self.oPgConnect.conn
        cursor=self.oPgConnect.cursor
        
        str_field_names=oStrFielsAndValues.str_field_names
        list_field_values=oStrFielsAndValues.list_field_values
        str_s_values=oStrFielsAndValues.str_s_values

        #cons_ins='insert into {0} ({1}) values (%s,st_geometryfromtext(%s,25830))'.format(nom_tabla, string_fields_to_set)
        cons_ins='insert into {0} ({1}) values ({2})'.format(nom_tabla, str_field_names, str_s_values)
        
        if str_fields_returning <> None:
            cons_ins =cons_ins + ' returning ' + str_fields_returning
        print 'Inserting'
        print cons_ins
        self.query=cons_ins
        cursor.execute(cons_ins,list_field_values)
        conn.commit()
        if str_fields_returning <> None:
            returning=cursor.fetchall()
            return returning
    
    def pgUpdate(self, table_name, oStrFielsAndValues, cond_where=None, list_values_cond_where=None):
        """
        Updates a table
        
        @type  table_name: string
        @param table_name: table name included the schema. Ej. "d.linde". 
            Mandatory specify the schema name: public.tablename
        @type oStrFielsAndValues: StrFielsAndValues
        @param oStrFielsAndValues: object with the field names and values to insert
        @type cond_where: string
        @param cond_where: the where condition to find the affected rows without values. 
            Instead of the values you have to put %s. e.g 'where area > %s and province = %s'
        @type list_values_cond_where: list
        @param list_values_cond_where: List of the values of the %s in the 
            parameter cond_where. e.g: [250, 'Alicante']
        @returns: The number of updated rows
        
        1. Example of use without dictionaries:
        import pgOperations2 as pgo
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
        
        
        Result
        
        Connected
        description, geom
        ['water well2', 'POINT(300 300)']
        %s,st_transform(st_geometryfromtext(%s,25830),25831)
        Query: update d.points set (description, geom) = (%s,st_transform(st_geometryfromtext(%s,25830),25831)) where gid=%s
        1
        
        2. Example of use with dictionaries, and automatic generation of expressions
        
        import pgOperations2 as pgo
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
        
        Result
        
        geom,description
        ['POINT(300 300)', 'water well2']
        st_transform(st_geometryfromtext(%s,25830),25831),%s
        Query: update d.points set (geom,description) = (st_transform(st_geometryfromtext(%s,25830),25831),%s) where gid=%s
        1  
        """
        conn=self.oPgConnect.conn
        cursor=self.oPgConnect.cursor

        str_field_names=oStrFielsAndValues.str_field_names
        list_field_values=oStrFielsAndValues.list_field_values
        str_s_values=oStrFielsAndValues.str_s_values
        
        cons='update {table_name} set ({str_field_names}) = ({str_s_values})'.format(table_name=table_name,str_field_names=str_field_names,str_s_values=str_s_values)
        if cond_where <> None:
            cons += ' ' + cond_where
            print 'Query: ' + cons
            cursor.execute(cons,list_field_values + list_values_cond_where)
        else:
            print 'Query: ' + cons
            cursor.execute(cons,list_field_values)
        conn.commit()
        self.query=cons
        return cursor.rowcount
    
    def pgDelete(self, table_name, cond_where=None, list_values_cond_where=None):
        """
        Deletes any row from any table. Example of use:
        
        @type  table_name: string
        @param table_name: table name included the schema. Ej. "d.linde". 
            Mandatory specify the schema name: public.tablename
        @type cond_where: string
        @param cond_where: the where condition to find the affected rows without values. 
            Instead of the values you have to put %s. e.g 'where area > %s and province = %s'
        @type list_values_cond_where: list
        @param list_values_cond_where: list of the values of the %s in the 
            parameter cond_where. e.g: [250, 'Alicante']
        @retuns: The number of deleted rows 
        
        Examples of use:
            #deletes the rows which gid < 4
            pgDelete2(table_name='d.buildings', cond_where='where gid < %s', list_values_cond_where=[4])
            #deletes all rows
            pg_delete2(table_name='d.buildings') 
        """
        conn=self.oPgConnect.conn
        cursor=self.oPgConnect.cursor
        cons='delete from {table_name}'.format(table_name=table_name)
        if cond_where <> None:
            cons += ' ' + cond_where
            print 'Query: ' + cons
            cursor.execute(cons, list_values_cond_where)
        else:
            print 'Query: ' + cons
            cursor.execute(cons)
        conn.commit()
        self.query=cons
        return cursor.rowcount
    
            
    def pgSelect(self, table_name, string_fields_to_select, cond_where='', list_val_cond_where=[]):
        """
        Select rows of a table
        
        @type  table_name: string
        @param table_name: table name included the schema. Ej. "d.linde". 
            Mandatory specify the schema name: public.tablename
        @type string_fields_to_select: string
        @param string_fields_to_select: string with the fields to select, comma separated. e.g: 'gid, descripcion, area, st_asgeojson(geom)'
        @type cond_where: string
        @param cond_where: the where condition to find the affected rows without values. 
            Instead of the values you have to put %s. e.g 'where area > %s and province = %s'
        @type list_values_cond_where: list
        @param list_values_cond_where: list of the values of the %s in the 
            parameter cond_where. e.g: [250, 'Alicante']        
        @return: 
            * None if there is not any selected row
            * a list of dictionaries fieldName:fieldValue. Each dictionary is a selected row
              to get the fist dictionary: lista[0]
              to get the second dictionary: lista[1]
              ... and so on
        
        Example of use:
        import pgOperations2 as pgo
        oCon=pgo.pgConnect(database="pruebas", user="postgres", password="postgres", host="localhost", port="5432")  
        oOp=pgo.pgOperations(oPgConnect=oCon)
        resp=oOp.pgSelect(table_name="d.points", 
                          string_fields_to_select='gid,depth,description,st_astext(geom)', 
                          cond_where = 'where gid > %s', 
                          list_val_cond_where=[0])
        print resp 
    
        Result
        
        Connected
        SELECT array_to_json(array_agg(registros)) FROM (select gid,depth,description,st_astext(geom) from d.points as t where gid > %s limit 100) as registros;
        [{u'depth': None, u'gid': 2, u'st_astext': u'POINT(-673652.157623927 202.793645345479)', u'description': u'water well'}, 
         {u'depth': 12.15, u'gid': 1, u'st_astext': u'POINT(-673449.363930927 304.189446432685)', u'description': u'water well2'}
        ]
        """
        cursor=self.oPgConnect.cursor
        
        #forms the select string
        cons='SELECT array_to_json(array_agg(registros)) FROM (select {string_fields_to_select} from {table_name} as t {cond_where} limit 100) as registros;'.format(string_fields_to_select=string_fields_to_select,table_name=table_name,cond_where=cond_where)      
        print cons
        self.query=cons
        #executes the string. The list_val_cond_where has the values of the %s in the select string by order
        if cond_where == '':
            cursor.execute(cons)
        else:
            cursor.execute(cons, list_val_cond_where)
        #gets all rows 
        lista = cursor.fetchall()
        r=lista[0][0]
        if r == None:
            return None #there wheren't selected rows
        else:
            #in ubuntu 14.04 r is a string, in 16.04 is a list
            #so if is string i convert it in list to return alwais a list
            if type(r) is str:
                r=json.loads(r)
            return r


    def getTableFieldNames(self, nomTable, changeGeomBySt_asgeojosonGeom=True, nomGeometryField='geom'):
        """
        Retuns a list with the table field names.
        @type  nomTable: string
        @param nomTable: table name included the schema. Ej. "d.linde". 
            Mandatory specify the schema name: public.tablename
        @type  changeGeomBySt_asgeojosonGeom: boolean
        @param changeGeomBySt_asgeojosonGeom: Specifies id the geom name field is changed by st_asgeojson(fieldName).     
        @type  nomGeometryField: string
        @param nomGeometryField: the geometry field name
        @return: A list with the table fiedl names
    
        Executes the sentence: 
        SELECT column_name FROM information_schema.columns WHERE table_schema='h30' and table_name = 'linde';
        
        Examples of use:
            listaCampos=getTableFieldNames('d.buildings')
                Returns: [u'gid', u'descripcion', u'area', 'st_asgeojson(geom)', u'fecha']
            listaCampos=getTableFieldNames(d.buildings', changeGeomBySt_asgeojosonGeom=False, nomGeometryField='geom')
                Returns: [u'gid', u'descripcion', u'area', u'geom', u'fecha']
        """
        
        consulta="SELECT column_name FROM information_schema.columns WHERE table_schema=%s and table_name = %s";
        
        lis=nomTable.split(".")
        
        cursor=self.oPgConnect.cursor
        cursor.execute(consulta,lis)
        if cursor.rowcount==0:
            return None
            
        listaValores=cursor.fetchall()#es una lista de tuplas.
                #cada tupla es una fila. En este caso, la fila tiene un
                #unico elemento, que es el nombre del campo.
        listaNombreCampos=[]
        for fila2 in listaValores:
            valor=fila2[0]
            if changeGeomBySt_asgeojosonGeom:
                if valor==nomGeometryField:
                    valor='st_asgeojson({0})'.format(nomGeometryField)
            listaNombreCampos.append(valor)
        self.query=consulta
        return listaNombreCampos   

class StrFielsAndValuesBase():
    """Class with tree properties uefull to the methods of the class pgOperations"""
    str_field_names=None
    """String with the name of the fields of a table. e.g: "depth, description, geom" - class variable"""
    list_field_values=None
    """List with the values of the fields. Exactry the same numbre and in the same order. e.g: [12.15, "water well","POINT(100 200)"] - class variable"""
    str_s_values=None
    """String with a %s for each field value, to suse in the execute method of the a psycopg2 cursor. e.g: "%s,%s,st_transform(st_geometryfromtext(%s,25830),25831)" """
    
    def __init__(self,str_field_names,list_field_values, str_s_values):
        self.str_field_names=str_field_names
        self.list_field_values=list_field_values
        self.str_s_values=str_s_values
        
class StrFielsAndValues(StrFielsAndValuesBase):
    """Class with tree properties uefull to the methods of the class pgOperations"""
    def __init__(self, 
                 d,
                 list_fields_to_remove=None, 
                 geom_field_name='geom', 
                 epsg='25830', 
                 geometry_type='POLYGON', 
                 epsg_to_reproject=None):
        """
        Initializes properties of the class               
        @type d: dictionary
        @param d: Dictionary key-value, where the keys are the name fields and the values the value fields of a table. 
            e.g: {"depth":12.15, "description":"water well", "geom":"100 200"}
            Pay attention in the geometry field value at this stage is a string coordinates "x y, x y, ..."
        @type list_fields_to_remove: list of strings
        @param list_fields_to_remove: list with the filed names to exclude of the expression. For example ['gid']
            will remove the gid from the expressions and list of values, as this field value is usually 
            automatically assigned by the database. None if you do not want to remove any field
        @type geom_field_name: string
        @param geom_field_name: name of the geometry field in the table. Has to be a key in the dictionary d. e.g "geom"
        @type epsg: string
        @param epsg: epsg code to assign to the geometry. e.g "25830"
        @type geometry_type: string
        @param geometry_type: POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON
        @type epsg_to_reproject: string
        @param epsg_to_reproject: epsg code to reproject the geometry. If the list of coordinates are in 25830 and you
            you want the geometries in 25831, you can do it. See the below examples. e.g: "25831". None if
            you do not want to reproject
        """
        
        StrFielsAndValuesBase.__init__(self, str_field_names=None, list_field_values=None, str_s_values=None)
        """Initialize  the base class properties to None"""
        self.__dict_to_string_fields_and_vector_values2(d,list_fields_to_remove, geom_field_name, epsg, geometry_type, epsg_to_reproject)
        """Change the class properties to the correct value"""
    def __dict_to_string_fields_and_vector_values2(
            self,
            d,
            list_fields_to_remove=None, 
            geom_field_name='geom', 
            epsg='25830', 
            geometry_type='POLYGON', 
            epsg_to_reproject=None):

        #remove the fileds to delete
        if list_fields_to_remove <> None:
            for i in range(len(list_fields_to_remove)):
                key=list_fields_to_remove[i]
                del d[key]
        
        #adds the geometry type and the paranthesis to the coordinates
        coords=d.get(geom_field_name,'')
        if coords<>'':#hay geometría
            if geometry_type=='POLYGON':
                coords='POLYGON(({coords}))'.format(coords=coords)
            elif geometry_type=='LINESTRING':
                coords='LINESTRING({coords})'.format(coords=coords)
            elif geometry_type=='POINT':
                coords='POINT({coords})'.format(coords=coords)
            elif geometry_type=='MULTIPOLYGON':
                coords='MULTIPOLYGON((({coords})))'.format(coords=coords)
            elif geometry_type=='MULTILINESTRING':
                coords='MULTILINESTRING(({coords}))'.format(coords=coords)
            elif geometry_type=='MULTIPOINT':
                coords='MULTIPOINT(({coords}))'.format(coords=coords)
            else:
                raise Exception("Unsuported geometry type " + geometry_type)
            d[geom_field_name]=coords
        
        #forms the tree values returned in the dictionary
        it=d.items()
        str_name_fields=''
        list_values =[]     
        str_s_values=''
        for i in range(len(it)):
            str_name_fields = str_name_fields + it[i][0] + ','
            #change the '' values by None
            if it[i][1]=='':
                list_values.append(None) 
            else:
                list_values.append(it[i][1])  
                
            if it[i][0] <> geom_field_name:
                str_s_values=str_s_values + '%s,'
            else:
                if epsg_to_reproject is None:
                    st='st_geometryfromtext(%s,{epsg}),'.format(epsg=epsg)
                    str_s_values=str_s_values + st
                else:
                    st='st_transform(st_geometryfromtext(%s,{epsg}),{epsg_to_reproject}),'.format(epsg=epsg, epsg_to_reproject=epsg_to_reproject)
                    str_s_values=str_s_values + st             
                #(%s,st_geometryfromtext(%s,25830))           
        str_name_fields=str_name_fields[:-1]
        str_s_values=str_s_values[:-1]
    
        self.str_field_names=str_name_fields
        self.list_field_values=list_values
        self.str_s_values=str_s_values
    
def transform_coords_ol_to_postgis(coords_geom, splitString=','):
    """
    Receives a string coordinate like 'x,y,x,y,x,y,....' from openlayers
    Returns a string like 'x y, x y, x y, ....'
    """
    lc=coords_geom.split(splitString)
    n=len(lc)
    sc=''
    for i in xrange(0,n,2):#starts in 0, stops in n, step 2
        #xrange(0,10,2)-->[0,2,4,6,8]
        x=lc[i]
        y=lc[i+1]
        sc=sc + ',' + x + ' ' + y
    return sc[1:]

def transform_coords_land_registry_gml_to_postgis(coords_geom, splitString=' '):
    """
    Receives a string coordinate like 'x,y x,y x,y,....' from land registry gml
    Returns a string like 'x y, x y, x y, ....'
    """
    
    coords_geom=coords_geom.replace(' ',',')
    return transform_coords_ol_to_postgis(coords_geom)

def reverseXY (strCoords, separatorIn, separatorOut): 
    """
    Changes the x y order for y x in a string
    Receives a string with 'x,y,x,y,...' 'or x y x y ...' and retuns a string with 'y,x,y,x,...'
        or 'y x y x ...'
    @type  strCoords: string
    @param strCoords: Coordenates string comma or space separated:
        'x,y,x,y,...' or 'x y x y ...'
    @type  separatorIn: string
    @param separatorIn: Coodinates separator in the input string. Can be ',' or ' '     
    @type  separatorOut: string
    @param separatorOut: Coodinates eparator in the output string. Can be ',' or ' '
    @return: a string with the 'y,x,y,x,...' or 'y x y x ...', depending of the separator
    
    Example of use:
        s='1,2,1,2,1,2'
        print reverseXY(s,',',',')
        '2,1,2,1,2,1'
    """
 
    
    if separatorIn==',':
        coords=strCoords.split(',')
    elif separatorIn==' ':
        coords=strCoords.split(' ')
    
    n=len(coords)
    r=''
    for i in xrange(0,n,2):#starts in 0, stops in n, step 2
        x=coords[i]
        y=coords[i+1]
        r=r + y + separatorOut + x + separatorOut
    return r[:-1]


def createDatabase(dbs):
    """
    Connects with the postgress database, and use the connection to creates the new database specified in the dbs dictionary
    the dbs dicctionary contains the following keys: user, password, database, port, host
    oConn is a pgOperation.pgConnection object. Adds to the new database, the extension postgis
    """
    oConn=pgConnect(database='postgres', user=dbs['user'], password=dbs['password'], 
                                 host=dbs['host'], port=dbs['port'])
    print "Creating database " + dbs['database']
    oConn.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    oConn.cursor.execute('create database ' + dbs['database'])
    oConn.disconnect()
    print "connecting to the new database " + dbs['database']
    oConn=pgConnect(database=dbs['database'], user=dbs['user'], password=dbs['password'], host=dbs['host'], port=dbs['port'])
    print "Creating the extension postgis into the database " + dbs['database']
    oConn.cursor.execute("create extension postgis;")
    oConn.conn.commit()
    oConn.disconnect()

def dropDatabase(dbs):
    """
    Connects with the postgress database, and use the connection to delete de database, whose name is in the dbs dictionary
    The dbs dicctionary contains the following keys: user, password, database, port, host
    """
    oConn=pgConnect(database='postgres', user=dbs['user'], password=dbs['password'], 
                                 host=dbs['host'], port=dbs['port'])
    print "Deleting database " + dbs['database']
    oConn.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    oConn.cursor.execute('drop database ' + dbs['database'])
    oConn.disconnect() 
