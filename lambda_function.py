
import odoolib
import os
import boto3
import pandas as pd
from pandas import json_normalize
import botocore
import json
from botocore.exceptions import ClientError
import logging
import time
import datetime


ODOO_USERNAME = os.environ['odoo_username']
ODOO_PASSWORD = os.environ['odoo_password']
ODOO_HOSTNAME = os.environ['odoo_hostname']
ODOO_DATABASE = os.environ['odoo_database']
BUCKET_NAME = os.environ['bucket_name_cencosud']
TIME_LIMIT = float(os.environ['tiempo_limite_ejecucion'])



class Producto:
    """
    Representa el producto o SKU

    ...

    Attributos:
    -----------
    unidadNegocio : objeto
        objeto unidad de negocio
    sku : str
        código identificador del producto
    ean : str
        código de barras del producto
    codRegional : str
        código regional del producto
    códRefProveedor : str
        código con el que el proveedor identifica el producto
    origen : selección
        "IMPORTADO / NACIONAL"
        muestra si el producto es importado o nacional
    marca : objeto
        marca asociada al producto
    proveedor : objeto   
        el proveedor asociado al producto
    actorRelevante : objeto
        actor relevante asociado al producto
    etapa : objeto (required)
        la etapa en la que se encuentra el producto

    Metodos:
    --------

    """

    def __init__(self,unidadNegocio,sku,ean,codRefProveedor,proveedor,descripcion,origen,marca,etapa,categoria1,categoria2,categoria3,unidadMedida=None,actorRelevante=None,codRegional=None,gerenciaNegocio=None):
        self.unidadNegocio = unidadNegocio
        self.sku = sku
        self.ean = ean #se inicializa el arreglo de eans
        self.descripcion = descripcion
        self.origen = origen
        self.marca = marca
        self.etapa = etapa        
        self.categoria1=categoria1
        self.categoria2=categoria2
        self.categoria3=categoria3
        self.unidadMedida=unidadMedida
        self.codRefProveedor = codRefProveedor
        self.proveedor = proveedor
        self.codRegional = codRegional
        self.gerenciaNegocio=gerenciaNegocio
        self.actorRelevante=actorRelevante
        

    def agregarEAN(self,ean):
        if not self.buscarEAN(ean):
            self.ean.append(ean)
        else:
            return 0
    def buscarEAN(self,ean):
        for eans in self.ean:
            if eans==ean:
                return 1
        return 0
    def buscarProveedorProducto(self,nombreProveedor):
        for eans in self.ean:
            if eans.proveedor==nombreProveedor:
                return 1
        return None    
class Venta:
    """
    Representa la unidad de venta de un producto o SKU
    ...

    Attributos:
    -----------
    year:
    mes:
    idProducto:
    descripcionProducto:
    unidadNegocio:
    ventaPrecencialJumbo:
    ventaEcommerceJumbo:
    ventaPrecencialConv:
    ventaEcommerceConv:
    ventaPrecencialSisa:
    ventaEcommerceSisa:
    ventaPrecencialEasy:
    ventaInternetEasy:
    ventaMayoristaEasy:
    ventaPrecencialParis:
    ventaEcommerceParis:

    Metodos:
    --------

    """

    def __init__(self,year,mes,producto, unidadNegocio,ventaPrecencialJumbo=0,ventaEcommerceJumbo=0,ventaPrecencialConv=0,ventaEcommerceConv=0,ventaPrecencialSisa=0,ventaEcommerceSisa=0,ventaPrecencialEasy=0,ventaInternetEasy=0,ventaMayoristaEasy=0,ventaPrecencialParis=0,ventaEcommerceParis=0):
        self.periodo = year
        self.mes = mes        
        self.producto = producto # HACER CAMBIO PARA GUARDAR ID DE PRODUCTO Y NO SKU
        self.unidadNegocio = unidadNegocio
        self.ventaPrecencialJumbo = ventaPrecencialJumbo
        self.ventaEcommerceJumbo = ventaEcommerceJumbo
        self.ventaPrecencialConv = ventaPrecencialConv        
        self.ventaEcommerceConv=ventaEcommerceConv
        self.ventaPrecencialSisa=ventaPrecencialSisa
        self.ventaEcommerceSisa=ventaEcommerceSisa
        self.ventaPrecencialEasy=ventaPrecencialEasy
        self.ventaInternetEasy = ventaInternetEasy
        self.ventaMayoristaEasy = ventaMayoristaEasy
        self.ventaPrecencialParis = ventaPrecencialParis
        self.ventaEcommerceParis = ventaEcommerceParis
class Ean:
    def __init__(self,codEAN,idSku,idProveedor):
        self.codEAN=codEAN
        self.idSku=idSku
        self.idProveedor=idProveedor
        pass
class RegistroDF:
    def __init__(self,keyobj,unidadNegocio,tipo):
        self.keyobj=keyobj
        self.unidadNegocio=unidadNegocio
        self.tipo=tipo
        pass

class Temporizador:
    def __init__(self,tiempoLimite):
        self.tiempoInicio=time.time()
        self.tiempoLimite=tiempoLimite
        pass
    def verificarTiempoLimite(self,tiempo):
        if tiempo-self.tiempoInicio<self.tiempoLimite:
            return True
        else:
            return False

class Proveedor:
    """
    Representa a un proveedor
    ...

    Attributos:
    -----------
    nombre : str (required)
        nombre del proveedor
    codProveedor : str
        el código de identificación del proveedor
    correo : str (required)
        el correo de contacto del proveedor

    Metodos:
    --------
    """
    def __init__(self,nombre,codProveedor,correo):
        self.nombre = nombre
        self.codProveedor = codProveedor
        self.correo = correo

        pass
class ActorRelevante:
    """
    Representa a un actor relevante

    Attributos:
    -----------
    nombre : str
        el nombre del actor relevante
    equipo : objeto
        equipo al que pertenece el actor relevante
    correo : objeto
        el correo asociado al actor relevante

    Metodos:
    --------
    """
    def __init__(self,nombre,equipo,correo):
        self.nombre = nombre
        self.equipo = equipo
        self.correo = correo

        pass

class DataFrame:
    def __init__(self,tipo,dataFrame,unidadNegocio,keyObj,nombrebucket):
        self.tipo=tipo
        self.unidadNegocio=unidadNegocio
        self.dataFrame=dataFrame
        self.keyobj=keyObj
        self.nombrebucket=nombrebucket
        pass
class Aws:
    def __init__(self):
        #self.s3_resource=boto3.resource('s3')
        self.s3_resource=boto3.resource('s3',aws_access_key_id='',aws_secret_access_key='')
    def obtenerBucket(self,nombreBucket):
        bucket=self.s3_resource.Bucket(nombreBucket)
        if bucket:
            return bucket
    def getListadoArchivos(self,bucket, prefix=None):
        objetos=bucket.objects.filter(Prefix=prefix)
        print(objetos)
        for obj in objetos:
            print(obj.key)
        return objetos
    def descargarArchivo(self,bucket,keyObjeto,rutaDestinoArchivo=None):
        print('intentando iniciar la descarga de '+keyObjeto)
        
        '''
        if self.obtenerBucket(nombreBucket).download_file(Key=nombreArchivo,Filename="/"+nombreArchivo):
            print('...Documento '+nombreArchivo+' descargado en '+rutaDestinoArchivo)
        else:
            print('...Descarga fallida')
        '''
        try:
            '''with open('filename', 'wb') as data:
                bucket.download_fileobj(keyObjeto,rutaDestinoArchivo, data)'''
            bucket.download_file(keyObjeto, rutaDestinoArchivo)
            print('...Documento '+keyObjeto+' descargado')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("El objeto no existe")
                print(e)
            else:
                raise

    def getNombreObjetoDB(self,obj,unidadNeg):
        if unidadNeg=='SMK':
            if obj.key.find('_LeyRep_Producto_SM')>0:
                return 'producto_SM'
            elif obj.key.find('_LeyRep_Insumo_SM')>0:
                return 'insumo_SM'
            elif obj.key.find('_LeyRep_Venta_SM')>0:
                return 'venta_SM'
            else:
                return 'Nombre_archivo_no_reconocido'
        elif unidadNeg=='MDH':
            if obj.key.find('_LeyRep_Producto_MDH')>0:
                return 'producto_MDH'
            elif obj.key.find('_LeyRep_Insumo_MDH')>0:
                return 'insumo_MDH'
            elif obj.key.find('_LeyRep_Venta_MDH')>0:
                return 'venta_MDH'
            else:
                return 'Nombre_archivo_no_reconocido'
        elif unidadNeg=='TXD':
            if obj.key.find('_LeyRep_TXD_Venta')>0:
                return 'producto_TXD'
            elif obj.key.find('_LeyRep_Insumo_TXD')>0:
                return 'insumo_TXD'
            else:
                return 'Nombre_archivo_no_reconocido'

        elif unidadNeg=='CORPORATIVO':

            valor =obj.key.find('SIG_SISA_SMK(NO_COMPLETADO)')
            if obj.key.find('SIG_SISA_SMK(NO_COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return nombre
            elif obj.key.find('JUMBO-CONVENIENCIA_SMK(NO_COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return nombre
            elif obj.key.find('SIG_MDH(NO_COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return nombre
            elif obj.key.find('SIG_TXD(NO_COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return nombre
            elif obj.key.find('SIG_SISA_SMK(COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return 'Nombre_archivo_no_reconocido'
            elif obj.key.find('SIG_JUMBO-CONVENIENCIA_SMK(COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return 'Nombre_archivo_no_reconocido'
            elif obj.key.find('SIG_MDH(COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return 'Nombre_archivo_no_reconocido'
            elif obj.key.find('SIG_TXD(COMPLETADO)')>0:
                nombre=obj.key.replace('CORPORATIVO/','')
                return 'Nombre_archivo_no_reconocido'
            
            else:
                return 'Nombre_archivo_no_reconocido'
        else:
            return 'Nombre_archivo_no_reconocido'
    
    def cambiarDocumentoFolder(self,nombreBucket,keyObj,rutaDestino,unidadNegocio):
        copy_source = {
            'Bucket': nombreBucket,
            'Key': keyObj
        }
        nombre=keyObj.replace(unidadNegocio+'/','')
        self.s3_resource.meta.client.copy(copy_source, nombreBucket, rutaDestino+nombre)
        self.s3_resource.meta.client.delete_object(
            Bucket=nombreBucket,
            Key=keyObj
        )

    def upload_file(self,file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        s3_client = self.s3_resource.meta.client
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    def obtenerDataFrames(self,unidadNeg,nombreBucket,tipo):
        bb=self.obtenerBucket(nombreBucket)
        listadoArchivos=self.getListadoArchivos(bb,unidadNeg+'/')
        dataFrames=[]
        if listadoArchivos:        
            for archivo in listadoArchivos:
                key= archivo.key
                nombreDocumento= key.replace(unidadNeg+'/','')
                if not archivo.key==unidadNeg+'/':
                    nombreobj=self.getNombreObjetoDB(archivo,unidadNeg)
                    rutaDestino='/tmp/'+nombreobj
                    valor=archivo.key.find('(COMPLETADO)')
                    if valor==-1:
                        self.descargarArchivo(bb,archivo.key,rutaDestino)
                        df = DataFrame(self.getNombreObjetoDB(archivo,unidadNeg),cargarDataFrame(rutaDestino,tipo),unidadNeg,archivo.key,nombreBucket)
                        dataFrames.append(df)
            return dataFrames
        return None
class Odoo:
    #PARÁMETROS INICIALES
    def __init__(self,hostname,database,login,password):
        self.conexion = odoolib.get_connection(
            hostname=hostname,
            database=database,
            login=login,
            password=password,
            port=443,
            protocol='jsonrpcs'
        )
        self.productos = self.conexion.get_model('x_productos')
        self.proveedor = self.conexion.get_model('res.partner')
        self.marca = self.conexion.get_model('x_marcas')
        self.actorRelevante = self.conexion.get_model('x_actores_relevantes')
        self.categoriaArea = self.conexion.get_model('x_area')
        self.categoriaDepartamento = self.conexion.get_model('x_departamento')
        self.categoriaSubdepartamento = self.conexion.get_model('x_subdepartamento')
        self.categoriaCategoria = self.conexion.get_model('x_categoria')
        self.categoriaSubcategoria = self.conexion.get_model('x_subcategoria')
        self.categoriaSeccion = self.conexion.get_model('x_seccion')
        self.categoriaRubro = self.conexion.get_model('x_rubro')
        self.categoriaSubrubro = self.conexion.get_model('x_subrubro')
        self.categoriaGrupo = self.conexion.get_model('x_grupo')
        self.equipo = self.conexion.get_model('x_equipos')
        self.unidadesNegocio=self.conexion.get_model('x_accesos_unidades_de_')
        self.ean=self.conexion.get_model('x_ean')
        self.etapa=self.conexion.get_model('x_productos_stage')
        self.ventas=self.conexion.get_model('x_ventas_sftp')
        self.periodos=self.conexion.get_model('x_periodo')
        self.registrosDF=self.conexion.get_model('x_registro_df')
        self.ventaTotal=self.conexion.get_model('x_ventas')
        self.materialidad=self.conexion.get_model('x_materialidad')
        self.declaracionesSIG=self.conexion.get_model('x_declaracion_sig')
        self.ecotasa=self.conexion.get_model('x_ecotasa_sku')
        self.grupoEcotasa=self.conexion.get_model('x_ecotasa_grupos_skus')
        #conjuntos de registros
        
        '''self.conjuntoProductosSMK=self.traerProductos('SMK')
        self.conjuntoProductosMDH=self.traerProductos('MDH')
        self.conjuntoProductosTXD=self.traerProductos('TXD')
        self.conjuntoProveedores=self.traerProveedores()
        self.conjuntoEan=self.traerEAN()
        self.conjuntoMarcas=self.traerMarcas()
        self.conjuntoDepartamentos=self.traerCategorias('x_departamento')
        self.conjuntoSubDepartamentos=self.traerCategorias('x_subdepartamento')
        self.conjuntocategorias=self.traerCategorias('x_categoria')
        self.conjuntoSubcategorias=self.traerCategorias('x_subcategoria')
        self.conjuntoSecciones=self.traerCategorias('x_seccion')
        self.conjuntoRubro=self.traerCategorias('x_rubro')
        self.conjuntoSubRubro=self.traerCategorias('x_subrubro')
        self.conjuntoGrupo=self.traerCategorias('x_grupo')
        self.conjuntoVentasSMK=self.traerVentas('SMK')
        self.conjuntoVentasMDH=self.traerVentas('MDH')
        self.conjuntoVentasTXD=self.traerVentas('TXD')'''
        self.conjuntoPeriodos=self.traerPeriodos()
        self.conjuntoActores=self.traerActores()
        self.conjutoDeclaracionesSIG=self.traerDeclaracionesSIG()
        self.conjuntoEcotasa=self.traerEcotasa()
        self.conjuntoGrupoEcotasa=self.traerGrupoEcotasa()
        print('Creando conjuntos de elementos')
        self.conjuntoMaterialidad= self.traerMaterialidad()
        print('Objeto establecido')
    #BUSCADORES
    def traerPeriodos(self):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve una lista de ids de registros con la llave primaria de la unidad de negocio correspondiente

        """
        try:
            resultadoBusqueda=self.periodos.search_read([],['x_name'])
            return resultadoBusqueda
        except IndexError:
            return None
    def traerProductos(self,unidadNegocio):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve una lista de ids de registros con la llave primaria de la unidad de negocio correspondiente

        """
        try:
            resultadoBusqueda=self.productos.search_read([('x_studio_unidades_de_negocio','=',unidadNegocio)],['x_name','x_studio_sku_unidad_de_negocio','x_studio_pm_asociado'])
            return resultadoBusqueda
        except IndexError:
            return None
    def traerVentas(self,unidadNegocio):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve una lista de ids de registros con la llave primaria de la unidad de negocio correspondiente

        """
        try:
            resultadoBusqueda=self.ventas.search_read([('x_studio_unidades_de_negocio','=',unidadNegocio)],['x_name','x_studio_periodo','x_studio_mes','x_studio_producto','x_studio_producto','x_studio_unidades_de_negocio','x_studio_ventaprecencialjumbo','x_studio_ventaecommercejumbo','x_studio_ventaprecencialconv','x_studio_ventaecommerceconv','x_studio_ventaprecencialsisa','x_studio_ventaecommercesisa','x_studio_ventaprecencialeasy','x_studio_ventainterneteasy','x_studio_ventamayoristaeasy','x_studio_ventaprecencialparis'])
            return resultadoBusqueda
        except IndexError:
            return None
    def traerProveedores(self):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve una lista de ids de registros con la llave primaria

        """
        try:
            resultadoBusqueda=self.proveedor.search_read([],['name','email'])
            return resultadoBusqueda
        except IndexError:
            return None
    def traerEAN(self,unidadNegocio=None):
        if unidadNegocio!=None:

            try:
                resultadoBusqueda=self.ean.search_read([('x_studio_unidades_de_negocio','=',unidadNegocio)],["x_name","x_studio_sku","x_studio_proveedor"])
                #resultadoBusqueda=str(self.ean.read(self.ean.search([('x_active','=',1)]),["x_name","x_studio_sku","x_studio_proveedor"]))

                return resultadoBusqueda
            except IndexError:
                return None
        else:
            try:
                resultadoBusqueda=self.ean.search_read([],["x_name","x_studio_sku","x_studio_proveedor"])
                #resultadoBusqueda=str(self.ean.read(self.ean.search([('x_active','=',1)]),["x_name","x_studio_sku","x_studio_proveedor"]))

                return resultadoBusqueda
            except IndexError:
                return None
    def traerCategorias(self,getModel):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve una lista de ids de registros con la llave primaria

        """
        
        try:
            cat=self.conexion.get_model(getModel)
            resultadoBusqueda=cat.search_read([],['x_name'])
            return resultadoBusqueda
        except IndexError:
            return None
        
        pass
    def traerMarcas(self):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve una lista de ids de registros con la llave primaria

        """
        try:
            resultadoBusqueda=self.marca.search_read([],['x_name'])
            return resultadoBusqueda
        except IndexError:
            return None
    def traerActores(self):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve una lista de ids de registros con la llave primaria

        """
        try:
            resultadoBusqueda=self.actorRelevante.search_read([],['x_name'])
            return resultadoBusqueda
        except IndexError:
            return None
    def traerEcotasa(self):


        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve una lista de ids de registros con la llave primaria

        """
        try:
            resultadoBusqueda=self.ecotasa.search_read([],['create_date',
                                                'x_studio_sku_unidad_de_negocio',
                                                'x_studio_valor_ecotasa_uf_cifras_significativas',
                                                'x_studio_valor_ecotasa_uf',
                                                'x_studio_e',
                                                ])
            return resultadoBusqueda
        except IndexError:
            return None

    def traerGrupoEcotasa(self):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve una lista de ids de registros con la llave primaria

        """
        try:
            resultadoBusqueda=self.grupoEcotasa.search_read([],['x_name',
                                                                     'x_studio_productos_seleccin_masiva',
                                                                     'x_studio_registros_para_calculo',
                                                                     'x_studio_robot_generador',
                                                                     'x_studio_unidad_de_negocio',
                                                                     'x_studio_documento_generado',
                                                                     ])
            return resultadoBusqueda
        except IndexError:
            return None
    def traerVentasTotales(self,unidadNegocio,periodo):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve una lista de ids de registros con la llave primaria de la unidad de negocio correspondiente

        """
        try:



            
            resultadoBusqueda=self.ventaTotal.search_read(
                ['&',('x_studio_unidades_de_negocio','=',unidadNegocio),('x_studio_periodo.x_name','=',periodo)],
                ['x_active',
                 'x_name',
                 'x_studio_producto',
                 'x_studio_sku_unidad_de_negocio',
                 'x_studio_descripcin_producto',
                 'x_studio_periodo',
                 'x_studio_unidades_de_negocio',
                 'x_studio_total_jumbo',
                 'x_studio_total_sisa',
                 'x_studio_total_conveniencia',
                 'x_studio_total_easy',
                 'x_studio_total_paris',
                 'x_studio_elementos_del_producto',
                ]
            )
            return resultadoBusqueda
        except IndexError:
            return None
    def traerMaterialidad(self):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve una lista de ids de registros con la llave primaria de la unidad de negocio correspondiente

        """
        try:
            '''resultadoBusqueda=self.materialidad.search_read(
                [("x_active","=",1)],
                ["x_active",
                 "x_studio_sku_unidad_de_negocio",
                 "x_name",
                 "x_studio_productos_por_envase",
                 "x_studio_peso",
                 "x_studio_peso_informado",
                 "x_studio_mat",
                 "x_studio_caractertica_del_material_solo_para_plsticos",
                 "x_studio_definir_otro_material",
                 "x_studio_caracterstica_reciclable",
                 "x_studio_caracteristica_retornable",
                 "x_studio_peligrosidad",
                 "x_studio_categora",
                 "x_studio_cat_material",
                 "x_studio_descripcin_sku",
                 ])'''
            

            idRegistros=self.materialidad.search(['|',('x_studio_categora','=','EYE Domiciliario'),('x_studio_categora','=','EYE No domiciliario')])
            resultadoBusqueda1=self.materialidad.read(idRegistros,
                ["x_active",
                 "x_studio_sku_unidad_de_negocio",
                 "x_studio_producto",
                 "x_name",
                 "x_studio_productos_por_envase",
                 "x_studio_peso",
                 "x_studio_peso_informado",
                 "x_studio_mat",
                 ])
            resultadoBusqueda2=self.materialidad.read(idRegistros,
                ["x_studio_mat",
                 "x_studio_caractertica_del_material_solo_para_plsticos",
                 "x_studio_definir_otro_material",
                 "x_studio_caracterstica_reciclable",
                 "x_studio_caracteristica_retornable",
                 "x_studio_peligrosidad",
                 "x_studio_categora",
                 "x_studio_cat_material",
                 "x_studio_descripcin_sku",
                 ])
            return [resultadoBusqueda1,resultadoBusqueda2]
        except IndexError:
            return None
        except json.decoder.JSONDecodeError:
            print('json inválido para materialidad')
        pass
    
    def traerDeclaracionesSIG(self):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve una lista de ids de registros con la llave primaria de la unidad de negocio correspondiente

        """
        try:
            resultadoBusqueda=self.declaracionesSIG.search_read(
                [],
                ['x_active',
                 'x_studio_unidad_de_negocio',
                 'x_studio_periodo',
                 'x_studio_razon_social',
                 'x_studio_generar_detalle_elementos',
                 'x_studio_doc_generado',
                ]
            )
            return resultadoBusqueda
        except IndexError:
            return None
        pass



    def buscarProductoRam(self,skuUnidadNegocio,unidadNegocio):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve la id del registro

        """
        if unidadNegocio=='SMK':
            listaRegistros=self.conjuntoProductosSMK
        elif unidadNegocio=='MDH':
            listaRegistros=self.conjuntoProductosMDH
        elif unidadNegocio=='TXD':
            listaRegistros=self.conjuntoProductosTXD

        for fila in range(len(listaRegistros)):
            skuUN=listaRegistros[fila]['x_studio_sku_unidad_de_negocio']
            if skuUN==skuUnidadNegocio:
                return listaRegistros[fila]['id']
        return None
    def buscarProductoRamId(self,idProducto,unidadNegocio):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve el SKU-unidad de negocio del registro

        """
        listaRegistros=[]
        if unidadNegocio=='SMK':
            listaRegistros=self.conjuntoProductosSMK
        elif unidadNegocio=='MDH':
            listaRegistros=self.conjuntoProductosMDH
        elif unidadNegocio=='TXD':
            listaRegistros=self.conjuntoProductosTXD

        for fila in range(len(listaRegistros)):
            skuUN=listaRegistros[fila]['id']
            if skuUN==idProducto:
                return listaRegistros[fila]['x_studio_sku_unidad_de_negocio']
        return None
    def buscarVentaRam(self,venta):
        listaRegistros=[]
        if venta.unidadNegocio=='SMK':
            listaRegistros=self.conjuntoVentasSMK
        elif venta.unidadNegocio=='MDH':
            listaRegistros=self.conjuntoVentasMDH
        elif venta.unidadNegocio=='TXD':
            listaRegistros=self.conjuntoVentasTXD
        for fila in range(len(listaRegistros)):
            sKU=listaRegistros[fila]['x_studio_producto'][1]
            periodo=listaRegistros[fila]['x_studio_periodo'][1]
            mes=listaRegistros[fila]['x_studio_mes']
            if sKU==str(venta.producto)+venta.unidadNegocio and periodo==venta.periodo and mes==venta.mes:
                return listaRegistros[fila]['id']
        return None        
    def buscarPeriodoRam(self,periodo):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve la id del registro

        """
        listaRegistros=self.conjuntoPeriodos
        for fila in range(len(listaRegistros)):
            per=listaRegistros[fila]['x_name']
            if per==periodo:
                return listaRegistros[fila]['id']
        return None
    def buscarActorRam(self,nombreActor):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve la id del registro

        """
        listaRegistros=self.conjuntoActores
        for fila in range(len(listaRegistros)):
            actor=listaRegistros[fila]['x_name']
            if actor==nombreActor:
                return listaRegistros[fila]['id']
        return None
    
    
    
    def buscarConjuntoEcotasa(self,idGrupoEcotasa):
        idResultado=self.ecotasa.search([])
        resultadoBusqueda=self.ecotasa.read(idResultado,['create_date',
                                            'x_studio_sku_unidad_de_negocio',
                                            'x_studio_valor_ecotasa_uf_cifras_significativas',
                                            'x_studio_valor_ecotasa_uf',
                                            'x_studio_e',
                                            ])
        return resultadoBusqueda


    
    
    
    def buscarEanRam(self,codEAN,codSku, unidadNegocio=None):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve la id del registro

        """
        listaRegistros=self.conjuntoEan
        for fila in range(len(listaRegistros)):
            cEan=listaRegistros[fila]['x_name']
            cSku=listaRegistros[fila]['x_studio_sku'][1]
            if cEan==codEAN and codSku==cSku:
                return listaRegistros[fila]['id']
        return None
    def buscarEanRamId(self,idEAN, unidadNegocio=None):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve la id del registro

        """
        listaRegistros=self.conjuntoEan
        for fila in range(len(listaRegistros)):
            iEan=listaRegistros[fila]['id']
            if iEan==idEAN:
                return listaRegistros[fila]['x_name']
        return None
    def buscarProveedorRam(self,nombreProveedor):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve la id del registro

        """
        listaregistros=self.conjuntoProveedores
        for fila in range(len(listaregistros)):
            nProveedor=listaregistros[fila]['name']
            if nProveedor==nombreProveedor:
                return listaregistros[fila]['id']
        return None
    def buscarProveedorRamId(self,idProveedor):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve la id del registro

        """
        listaregistros=self.conjuntoProveedores
        for fila in range(len(listaregistros)):
            nProveedor=listaregistros[fila]['id']
            if nProveedor==idProveedor:
                return listaregistros[fila]['name']
        return None
    def buscarMarcaRam(self,nombreMarca):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve la id del registro

        """
        listaregistros=self.conjuntoMarcas
        for fila in range(len(listaregistros)):
            nMarca=listaregistros[fila]['x_name']
            if nMarca==nombreMarca:
                return listaregistros[fila]['id']
        return None
    def buscarMarcaRamId(self,idMarca):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve la id del registro

        """
        listaregistros=self.conjuntoMarcas
        for fila in range(len(listaregistros)):
            nMarca=listaregistros[fila]['id']
            if nMarca==idMarca:
                return listaregistros[fila]['x_name']
        return None
    def buscarCategoriaRam(self,listaregistros,nombreCategoria):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve la id del registro

        """
        for fila in range(len(listaregistros)):
            nCategoria=listaregistros[fila]['x_name']
            if nCategoria==nombreCategoria:
                return listaregistros[fila]['id']
        return None
    def buscarCategoriaRamId(self,listaregistros,idCategoria):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve la id del registro

        """
        for fila in range(len(listaregistros)):
            nCategoria=listaregistros[fila]['id']
            if nCategoria==idCategoria:
                return listaregistros[fila]['x_name']
        return None
    
    def buscarMaterialidadesIDSku(self,idSku):
        """
        Descripción metodo

        Parámetros:
        parámetro1:
        parámetro2: 

        Returns: devuelve la id del registro

        """
        listaregistros=self.conjuntoMaterialidad[0]
        listaSelecionados1=[]
        listaSelecionados2=[]
        for fila in range(len(listaregistros)):
            reg=listaregistros[fila]['x_studio_producto']
            if reg:
                if idSku==listaregistros[fila]['x_studio_producto'][0]:
                    listaSelecionados1.append(listaregistros[fila])
                    listaSelecionados2.append(self.conjuntoMaterialidad[1][fila])
        return [listaSelecionados1,listaSelecionados2]
    def buscarMaterialidadId(self,idMaterial):
        listaregistros=self.conjuntroMaterialidad
        for fila in range(len(listaregistros)):
            if idMaterial==listaregistros[fila]['id']:
                return listaregistros[fila]
        return None





    def buscarProductos(self,codProducto,unidadNegocio):
        """
        busca un producto en la base de datos

        Parámetros:
        -codProducto: código del producto que se necesita encontrar
        -conexion: función conexión

        Returns: devuelve el nombre del producto, la descripción y el proveedor al que pertenece

        """
        try:
            resultadoBusqueda=self.productos.read(self.productos.search(['&',('x_studio_sku_unidad_de_negocio','=',str(codProducto)),('x_studio_unidades_de_negocio','=',unidadNegocio)])[0],["id","x_name", "x_studio_descripcin","x_studio_proveedor"])
            return resultadoBusqueda
        except IndexError:
            return None
    def buscarProductosId(self,idProducto):
        """
        hace la búsqueda del producto, según la id

        Parámetros:
        -idProducto: la id del producto que se está buscando
        -conexion: función conexión

        Returns: entrega el nombre del producto, la descripción y el proveedor al que pertenece

        """ 
        
        try:
            resultadoBusqueda=self.productos.read(self.productos.search([('id','=',idProducto)])[0],["x_name", "x_studio_descripcin","x_studio_proveedor"])
            return resultadoBusqueda

        except IndexError:
            return None
    def buscarProveedor(self,nombreProveedor):
        """
        hace la búsqueda de los datos del proveedor

        Parámetros:
        -nombreProveedor: el nombre del proveedor que se está buscando
        -conexion: función conexión

        Returns: entrega el nombre del proveedor y el correo electrónico

        """ 
        try:
            resultadoBusqueda=self.proveedor.read(self.proveedor.search([('name','=',nombreProveedor)])[0],["name", "email"])
            return resultadoBusqueda

        except IndexError:
            return None
    def buscarProveedorId(self,idProveedor):
        """
        hace la búsqueda de los datos del proveedor usando la id

        Parámetros:
        -idProveedor: la id del proveedor que se está buscando
        -conexion: función conexión

        Returns: entrega el nombre y correo electŕonico del proveedor buscado

        """ 
        try:
            resultadoBusqueda=self.proveedor.read(self.proveedor.search([('id','=',idProveedor)])[0],["name", "email"])
            return resultadoBusqueda

        except IndexError:
            return None
    def buscarEan(self,ean):
        try:
            resultadoBusqueda=self.ean.read(self.ean.search([('x_name','=',ean)])[0],["x_name"])
            return resultadoBusqueda

        except IndexError:
            return None
    def buscarPeriodo(self,periodo):
        conjPeriodo=self.conjuntoPeriodos
        for fila in range(len(conjPeriodo)):
            if conjPeriodo[fila]['x_name']==periodo:
                return conjPeriodo[fila]['id']
        return None

    def buscarMarca(self,nombreMarca):
        """
        hace la búsqueda de la marca

        Parámetros:
        -nombreMarca: el nombre de la marca
        -conexion: función conexión

        Returns: entrega el nobre de la marca

        """ 
        try:
            resultadoBusqueda=self.marca.read(self.marca.search([('x_name','=',nombreMarca)])[0],["x_name"])
            return resultadoBusqueda

        except IndexError:
            return None
    def buscarUN(self,nombreUN):
        """
        hace la búsqueda de la unidad de negocio

        Parámetros:
        -nombreUN: el nombre de la unidad de negocio
        -conexion: función conexión

        Returns: devuelve el nombre de la unidad de negocio

        """ 
        try:
            resultadoBusqueda=self.unidadesNegocio.read(self.unidadesNegocio.search([('x_name','=',nombreUN)])[0],["x_name"])
            return resultadoBusqueda

        except IndexError:
            return None
    def buscarEtapa(self,nombreEtapa):
        """
        hace la búsqueda de la etapa

        Parámetros:
        -nombreEtapa: el nombre de la etapa
        -conexion: función conexión

        Returns: hace entrega del nombre de la etapa

        """ 

        try:
            resultadoBusqueda=self.etapa.read(self.etapa.search([('x_name','=',nombreEtapa)])[0],["x_name"])
            return resultadoBusqueda
        except IndexError:
            return None
    def buscarMarcaId(self,idMarca):
        """
        hace la búsqueda de la marca según la id

        Parámetros:
        -idMarca: la id de la marca que se requiere
        -conexion: función conexión

        Returns: hace entrega del nombre de la marca

        """ 

        try:
            resultadoBusqueda=self.marca.read(self.marca.search([('id','=',idMarca)])[0],["x_name"])
            return resultadoBusqueda

        except IndexError:
            return None    
    def buscarActorRelevante(self,nombreActorRelevante):
        """
        hace la búsqueda del actor relevante

        Parámetros:
        -nombreActorRelevante: el nombre del actor relevante
        -conexion: función conexión

        Returns: hace la entrega de el nombre, el equipo, y el correo del actor relevante

        """ 

        try:
            resultadoBusqueda=self.actorRelevante.read(self.actorRelevante.search([('x_name','=',nombreActorRelevante)])[0],["x_name","x_studio_equipo","x_studio_partner_email"])
            return resultadoBusqueda

        except IndexError:
            return None
    def buscarActorRelevanteId(self,idActorRelevante):
        """
        hace la búsqueda del actor relevante a través de la id

        Parámetros:
        -idActorRelevante: la id del actor relevante
        -conexión: la función conexión

        Returns: hace entrega de el nombre, el equipo y el correo del actor relevante

        """ 

        try:
            resultadoBusqueda=self.actorRelevante.read(self.actorRelevante.search([('id','=',idActorRelevante)])[0],["x_name","x_studio_equipo","x_studio_partner_email"])
            return resultadoBusqueda

        except IndexError:
                return None   
    def buscarEquipo(self,nombreEquipo):
        """
        hace la búsqueda del equipo
        
        Parámetros:
        -nombreEquipo: el nombre del equipo
        -conexion: función conexión

        Returns: entrega el nombre y la id del equipo

        """  
        try:
            resultadoBusqueda=self.equipo.read(self.equipo.search([('x_name','=',nombreEquipo)])[0],["id"])
            return resultadoBusqueda

        except IndexError:
            return None
    def buscarEquipoId(self,idEquipo):
        """
        hace la búsqueda de los datos del equipo con la id

        Parámetros:
        -idEquipo: id del equipo
        -conexion: función conexión

        Returns: devuelve el nombre y el área del equipo

        """ 
        try:
            resultadoBusqueda=self.equipo.read(self.equipo.search([('id','=',idEquipo)])[0],["x_name","x_studio_area"])
            return resultadoBusqueda

        except IndexError:
            return None 
    # GENERADORES:
    def crearProveedor(self,nuevoProveedor):
        """
        crea un proveedor, si este no existe

        Parámetros:
        -nuevoProveedor: el nuevo proveedor
        -conexion: función conexión

        Returns: devuelve un arreglo con la id y nombre del proveedor

        """ 
        if not self.buscarProveedorRam(nuevoProveedor.nombre):
        #if not self.buscarProveedor(nuevoProveedor.nombre):
            proveedorCreado = self.proveedor.create({
                'name' : nuevoProveedor.nombre,
                'email' : nuevoProveedor.correo,
                'x_studio_proveedor' : 1,
                'vat' : nuevoProveedor.codProveedor
            })
            self.conjuntoProveedores.append({'id': proveedorCreado, 'name': nuevoProveedor.nombre, 'email': nuevoProveedor.correo})
            return proveedorCreado
        else:
            return None
    def crearMarca(self,nuevaMarca):
        """
        crea la marca

        Parámetros:
        -nuevaMarca: la nueva marca
        -conexion: función conexión

        Returns: devuelve el nombre de la marca

        """ 
        if not self.buscarMarcaRam(nuevaMarca):
        #if not self.buscarMarca(nuevaMarca,conexion):
            marcaCreada = self.marca.create({
                'x_name' : nuevaMarca
            })
            self.conjuntoMarcas.append({'id': marcaCreada, 'x_name': nuevaMarca})
            return marcaCreada
        else:
            return None
    def crearActorRelevante(self,nuevoActorRelevante):
        """
        crea un actor relevante

        Parámetros:
        -nuevoActorRelevante: actor relevante a crear
        -conexion: función conexión

        Returns: devuelve el nombre, el equipo, y el correo del actor relevante

        """ 

        if not self.buscarActorRelevante(nuevoActorRelevante.nombre):
            equipo = self.buscarEquipo(nuevoActorRelevante.equipo) #Aqui es posible agregar un "if" para el caso que no existe el Equipo
            actorRelevanteCreado = self.actorRelevante.create({
                'x_name' : nuevoActorRelevante.nombre,
                'x_studio_equipo' : equipo["id"],
                'x_studio_partner_email' : nuevoActorRelevante.correo,
            })

            aux = []
            aux.append(self.buscarActorRelevanteId(actorRelevanteCreado))
            aux.append(actorRelevanteCreado)
            return aux
        else:
            return None
    def crearProducto(self,nuevoProducto):
        """
        crea un producto

        Parámetros:
        -nuevoProducto: el producto a crear
        -conexion: función conexión

        Returns: entrega el nombre del producto, la descripción y el proveedor

        """ 
        resultadoBusqueda=self.buscarProductoRam(nuevoProducto.sku,nuevoProducto.unidadNegocio)
        #resultadoBusqueda=self.buscarProductos(nuevoProducto.sku,nuevoProducto.unidadNegocio)
        if not resultadoBusqueda:
            #print("sku no presenta en la BD")
            #chequeo marca
            marca = self.buscarMarcaRam(nuevoProducto.marca)
            if not marca:
                nuevaMarca = nuevoProducto.marca
                marca = self.crearMarca(nuevaMarca)
            #chequeo UN
            #uN = self.buscarUN(nuevoProducto.unidadNegocio)
            #print(uN)
            #if not uN:
            #    print("Unidad de negocio no está presente en BD")

            #chequeo proveedor
            idProveedor=self.buscarProveedorRam(nuevoProducto.proveedor)
            #idProveedor = self.buscarProveedor(nuevoProducto.proveedor)
                        
            if not idProveedor:
                #print("PROVEEDOR no presenta en BD")
                prov = Proveedor(nuevoProducto.proveedor,'','')
                idProveedor = self.crearProveedor(prov)
            #chequeo categorias
            if nuevoProducto.unidadNegocio=='SMK':

                idcategoria1=self.buscarCategoriaRam(self.conjuntoDepartamentos,nuevoProducto.categoria1)
                if not idcategoria1:
                    idcategoria1 = self.crearDepartamento(nuevoProducto.categoria1)
                
                idcategoria2=self.buscarCategoriaRam(self.conjuntoSubDepartamentos,nuevoProducto.categoria2)
                if not idcategoria2:
                    idcategoria2 = self.crearSubDepartamento(nuevoProducto.categoria2)

                idcategoria3=self.buscarCategoriaRam(self.conjuntocategorias,nuevoProducto.categoria2)
                if not idcategoria3:
                    idcategoria3 = self.crearCategoria(nuevoProducto.categoria3)


            elif nuevoProducto.unidadNegocio=='MDH':

                idcategoria1=self.buscarCategoriaRam(self.conjuntoSecciones,nuevoProducto.categoria1)
                if not idcategoria1:
                    idcategoria1 = self.crearSeccion(nuevoProducto.categoria1)
                
                idcategoria2=self.buscarCategoriaRam(self.conjuntoRubro,nuevoProducto.categoria2)
                if not idcategoria2:
                    idcategoria2 = self.crearRubro(nuevoProducto.categoria2)

                idcategoria3=self.buscarCategoriaRam(self.conjuntoSubRubro,nuevoProducto.categoria2)
                if not idcategoria3:
                    idcategoria3 = self.crearSubRubro(nuevoProducto.categoria3)


            elif nuevoProducto.unidadNegocio=='TXD':
                idcategoria1=self.buscarCategoriaRam(self.conjuntoDepartamentos,nuevoProducto.categoria1)
                if not idcategoria1:
                    idcategoria1 = self.crearDepartamento(nuevoProducto.categoria1)

                idcategoria2=self.buscarCategoriaRam(self.conjuntoSubDepartamentos,nuevoProducto.categoria2)
                if not idcategoria2:
                    idcategoria2 = self.crearSubDepartamento(nuevoProducto.categoria2)

            #chequeo actor relevante
            #idActor = self.buscarActorRelevante(nuevoProducto.actorRelevante)
            #print(idActor)
            #if not idActor:
            #print("ACTOR no presenta en BD")
            #actor = ActorRelevante(nuevoProducto.actorRelevante,'','')
            #idActor = self.crearActorRelevante(actor)
            unidadesNeg =[]
            origen=nuevoProducto.origen
            if nuevoProducto.origen=='IMPORTADOS':
                origen='IMPORTADO'
            elif nuevoProducto.origen=='NACIONALES':
                origen='IMPORTADO'
            if nuevoProducto.unidadNegocio=="SMK":
                unidadesNeg.append(1)
                productoCreado = self.productos.create({
                    'x_studio_unidades_de_negocio' : unidadesNeg,
                    'x_name' : str(nuevoProducto.sku)+nuevoProducto.unidadNegocio,
                    'x_studio_sku_unidad_de_negocio':str(nuevoProducto.sku),
                    #'x_studio_ean_asociados': '',
                    #'x_studio_ean' : nuevoProducto.ean,
                    'x_studio_cdigo_regional' : nuevoProducto.codRegional,
                    'x_studio_departamento': idcategoria1,
                    'x_studio_subdepartamento': idcategoria2,
                    'x_studio_categora': idcategoria3,
                    'x_studio_cdigo_referencia_proveedor' : nuevoProducto.codRefProveedor,
                    'x_studio_descripcin': nuevoProducto.descripcion,
                    'x_studio_origen' : origen,
                    'x_studio_marca' : marca,
                    'x_studio_proveedor': idProveedor,
                    #'x_studio_pm_asociado': idActor['id'],
                    'x_studio_stage_id' : 2
                })
                self.conjuntoProductosSMK.append({'id': productoCreado, 'x_name': str(nuevoProducto.sku)+nuevoProducto.unidadNegocio, 'x_studio_sku_unidad_de_negocio': str(nuevoProducto.sku)})
            elif nuevoProducto.unidadNegocio=="MDH":
                unidadesNeg.append(2)
                productoCreado = self.productos.create({
                    'x_studio_unidades_de_negocio' : unidadesNeg,
                    'x_name' : str(nuevoProducto.sku)+nuevoProducto.unidadNegocio,
                    'x_studio_sku_unidad_de_negocio':str(nuevoProducto.sku),
                    #'x_studio_ean_asociados': '',
                    #'x_studio_ean' : nuevoProducto.ean,
                    'x_studio_cdigo_regional' : nuevoProducto.codRegional,
                    'x_studio_seccion': idcategoria1,
                    'x_studio_rubro': idcategoria2,
                    'x_studio_subrubro': idcategoria3,
                    #'x_studio_grupo': idcategoria3,
                    'x_studio_cdigo_referencia_proveedor' : nuevoProducto.codRefProveedor,
                    'x_studio_descripcin': nuevoProducto.descripcion,
                    'x_studio_origen' : nuevoProducto.origen,
                    'x_studio_marca' : marca,
                    #'x_studio_proveedor': idProveedor,
                    #'x_studio_pm_asociado': idActor['id'],
                    'x_studio_stage_id' : 2
                })
                self.conjuntoProductosMDH.append({'id': productoCreado, 'x_name': str(nuevoProducto.sku)+nuevoProducto.unidadNegocio, 'x_studio_sku_unidad_de_negocio': str(nuevoProducto.sku)})
            elif nuevoProducto.unidadNegocio=="TXD":
                unidadesNeg.append(3)
                productoCreado = self.productos.create({
                    'x_studio_unidades_de_negocio' : unidadesNeg,
                    'x_name' : str(nuevoProducto.sku)+nuevoProducto.unidadNegocio,
                    'x_studio_sku_unidad_de_negocio':str(nuevoProducto.sku),
                    #'x_studio_ean_asociados': '',
                    #'x_studio_ean' : nuevoProducto.ean,
                    'x_studio_cdigo_regional' : nuevoProducto.codRegional,
                    'x_studio_departamento_txd': idcategoria1,
                    'x_studio_subdepartamento_txd': idcategoria2,
                    'x_studio_cdigo_referencia_proveedor' : nuevoProducto.codRefProveedor,
                    'x_studio_descripcin': nuevoProducto.descripcion,
                    'x_studio_origen' : nuevoProducto.origen,
                    'x_studio_marca' : marca,
                    #'x_studio_proveedor': idProveedor,
                    #'x_studio_pm_asociado': idActor['id'],
                    'x_studio_stage_id' : 2
                })
                self.conjuntoProductosTXD.append({'id': productoCreado, 'x_name': str(nuevoProducto.sku)+nuevoProducto.unidadNegocio, 'x_studio_sku_unidad_de_negocio': str(nuevoProducto.sku)})
            self.crearEan(Ean(str(nuevoProducto.ean),productoCreado,idProveedor),nuevoProducto.unidadNegocio)
    def crearProductoVentaSMK(self,nuevoProducto):
        """
        crea un producto

        Parámetros:
        -nuevoProducto: el producto a crear
        -conexion: función conexión

        Returns: entrega el nombre del producto, la descripción y el proveedor

        """ 
        resultadoBusqueda=self.buscarProductoRam(nuevoProducto.sku,nuevoProducto.unidadNegocio)
        #resultadoBusqueda=self.buscarProductos(nuevoProducto.sku,nuevoProducto.unidadNegocio)
        if not resultadoBusqueda:
            unidadesNeg =[]
            if nuevoProducto.unidadNegocio=="SMK":
                unidadesNeg.append(1)
                productoCreado = self.productos.create({
                    'x_studio_unidades_de_negocio' : unidadesNeg,
                    'x_name' : str(nuevoProducto.sku)+nuevoProducto.unidadNegocio,
                    'x_studio_sku_unidad_de_negocio':str(nuevoProducto.sku),
                    'x_studio_stage_id' : 2
                })
                self.conjuntoProductosSMK.append({'id': productoCreado, 'x_name': str(nuevoProducto.sku)+nuevoProducto.unidadNegocio, 'x_studio_sku_unidad_de_negocio': str(nuevoProducto.sku)})
    def crearRegistroDF(self,registroDF):
        registroDFCreado = self.registrosDF.create({
            'x_studio_keyobj' : registroDF.keyobj,
            'x_studio_unidad_de_negocios':registroDF.unidadNegocio,
            'x_studio_tipo':registroDF.tipo
        })
        pass

    def crearProductoInsumo(self,nuevoProducto):
        """
        crea un producto

        Parámetros:
        -nuevoProducto: el producto a crear
        -conexion: función conexión

        Returns: entrega el nombre del producto, la descripción y el proveedor

        """ 
        resultadoBusqueda=self.buscarProductoRam(nuevoProducto.sku,nuevoProducto.unidadNegocio)
        #resultadoBusqueda=self.buscarProductos(nuevoProducto.sku,nuevoProducto.unidadNegocio)
        if not resultadoBusqueda:
            #print("sku no presenta en la BD")
            #chequeo marca
            marca = self.buscarMarcaRam(nuevoProducto.marca)
            if not marca:
                nuevaMarca = nuevoProducto.marca
                marca = self.crearMarca(nuevaMarca)
            #chequeo UN
            #uN = self.buscarUN(nuevoProducto.unidadNegocio)
            #print(uN)
            #if not uN:
            #    print("Unidad de negocio no está presente en BD")

            #chequeo proveedor
            idProveedor=self.buscarProveedorRam(nuevoProducto.proveedor)
            #idProveedor = self.buscarProveedor(nuevoProducto.proveedor)
                        
            if not idProveedor:
                #print("PROVEEDOR no presenta en BD")
                prov = Proveedor(nuevoProducto.proveedor,'','')
                idProveedor = self.crearProveedor(prov)
            #chequeo categorias
            if nuevoProducto.unidadNegocio=="SMK":
                
                idcategoria1=self.buscarCategoriaRam(self.conjuntoDepartamentos,nuevoProducto.categoria1)
                if not idcategoria1:
                    idcategoria1 = self.crearDepartamento(nuevoProducto.categoria1)
                
                idcategoria2=self.buscarCategoriaRam(self.conjuntoSubDepartamentos,nuevoProducto.categoria2)
                if not idcategoria2:
                    idcategoria2 = self.crearSubDepartamento(nuevoProducto.categoria2)

                idcategoria3=self.buscarCategoriaRam(self.conjuntocategorias,nuevoProducto.categoria2)
                if not idcategoria3:
                    idcategoria3 = self.crearCategoria(nuevoProducto.categoria3)


            elif nuevoProducto.unidadNegocio=="MDH":

                idcategoria1=self.buscarCategoriaRam(self.conjuntoSecciones,nuevoProducto.categoria1)
                if not idcategoria1:
                    idcategoria1 = self.crearSeccion(nuevoProducto.categoria1)
                
                idcategoria2=self.buscarCategoriaRam(self.conjuntoRubro,nuevoProducto.categoria2)
                if not idcategoria2:
                    idcategoria2 = self.crearRubro(nuevoProducto.categoria2)

                idcategoria3=self.buscarCategoriaRam(self.conjuntoSubRubro,nuevoProducto.categoria2)
                if not idcategoria3:
                    idcategoria3 = self.crearSubRubro(nuevoProducto.categoria3)


            elif nuevoProducto.unidadNegocio=="TXD":

                if not idcategoria1:
                    idcategoria1 = self.crearDepartamento(nuevoProducto.categoria1)

                idcategoria2=self.buscarCategoriaRam(self.conjuntoSubDepartamentos,nuevoProducto.categoria2)
                if not idcategoria2:
                    idcategoria2 = self.crearSubDepartamento(nuevoProducto.categoria2)

                idcategoria3=self.buscarCategoriaRam(self.conjuntocategorias,nuevoProducto.categoria2)
                if not idcategoria3:
                    idcategoria3 = self.crearCategoria(nuevoProducto.categoria3)
            #chequeo actor relevante
            #idActor = self.buscarActorRelevante(nuevoProducto.actorRelevante)
            #print(idActor)
            #if not idActor:
            #print("ACTOR no presenta en BD")
            #actor = ActorRelevante(nuevoProducto.actorRelevante,'','')
            #idActor = self.crearActorRelevante(actor)
            unidadesNeg =[]
            if nuevoProducto.unidadNegocio=="SMK":
                unidadesNeg.append(1)
                productoCreado = self.productos.create({
                    'x_studio_unidades_de_negocio' : unidadesNeg,
                    'x_name' : str(nuevoProducto.sku)+nuevoProducto.unidadNegocio,
                    'x_studio_sku_unidad_de_negocio':str(nuevoProducto.sku),
                    #'x_studio_ean_asociados': '',
                    #'x_studio_ean' : nuevoProducto.ean,
                    'x_studio_cdigo_regional' : nuevoProducto.codRegional,
                    'x_studio_departamento': idcategoria1,
                    'x_studio_subdepartamento': idcategoria2,
                    'x_studio_categora': idcategoria3,
                    'x_studio_cdigo_referencia_proveedor' : nuevoProducto.codRefProveedor,
                    'x_studio_descripcin': nuevoProducto.descripcion,
                    'x_studio_origen' : nuevoProducto.origen,
                    'x_studio_marca' : marca,
                    'x_studio_proveedor': idProveedor,
                    'x_studio_pm_asociado': self.buscarActorRam('Carolina Pino'),
                    'x_studio_stage_id' : 2
                })
                self.conjuntoProductosSMK.append({'id': productoCreado, 'x_name': str(nuevoProducto.sku)+nuevoProducto.unidadNegocio, 'x_studio_sku_unidad_de_negocio': str(nuevoProducto.sku)})
            elif nuevoProducto.unidadNegocio=="MDH":
                unidadesNeg.append(2)
                productoCreado = self.productos.create({
                    'x_studio_unidades_de_negocio' : unidadesNeg,
                    'x_name' : str(nuevoProducto.sku)+nuevoProducto.unidadNegocio,
                    'x_studio_sku_unidad_de_negocio':str(nuevoProducto.sku),
                    #'x_studio_ean_asociados': '',
                    #'x_studio_ean' : nuevoProducto.ean,
                    'x_studio_cdigo_regional' : nuevoProducto.codRegional,
                    'x_studio_seccion': idcategoria1,
                    'x_studio_rubro': idcategoria2,
                    'x_studio_subrubro': idcategoria3,
                    #'x_studio_grupo': idcategoria3,
                    'x_studio_cdigo_referencia_proveedor' : nuevoProducto.codRefProveedor,
                    'x_studio_descripcin': nuevoProducto.descripcion,
                    'x_studio_origen' : nuevoProducto.origen,
                    'x_studio_marca' : marca,
                    #'x_studio_proveedor': idProveedor,
                    #'x_studio_pm_asociado': idActor['id'],
                    'x_studio_stage_id' : 2
                })
                self.conjuntoProductosMDH.append({'id': productoCreado, 'x_name': str(nuevoProducto.sku)+nuevoProducto.unidadNegocio, 'x_studio_sku_unidad_de_negocio': str(nuevoProducto.sku)})
            elif nuevoProducto.unidadNegocio=="TXD":
                unidadesNeg.append(3)
                productoCreado = self.productos.create({
                    'x_studio_unidades_de_negocio' : unidadesNeg,
                    'x_name' : str(nuevoProducto.sku)+nuevoProducto.unidadNegocio,
                    'x_studio_sku_unidad_de_negocio':str(nuevoProducto.sku),
                    #'x_studio_ean_asociados': '',
                    #'x_studio_ean' : nuevoProducto.ean,
                    'x_studio_cdigo_regional' : nuevoProducto.codRegional,
                    'x_studio_departamento_txd': idcategoria1,
                    'x_studio_subdepartamento_txd': idcategoria2,
                    'x_studio_subcategora': idcategoria3,
                    'x_studio_cdigo_referencia_proveedor' : nuevoProducto.codRefProveedor,
                    'x_studio_descripcin': nuevoProducto.descripcion,
                    'x_studio_origen' : nuevoProducto.origen,
                    'x_studio_marca' : marca,
                    #'x_studio_proveedor': idProveedor,
                    #'x_studio_pm_asociado': idActor['id'],
                    'x_studio_stage_id' : 2
                })
                self.conjuntoProductosTXD.append({'id': productoCreado, 'x_name': str(nuevoProducto.sku)+nuevoProducto.unidadNegocio, 'x_studio_sku_unidad_de_negocio': str(nuevoProducto.sku)})
            self.crearEan(Ean(str(nuevoProducto.ean),productoCreado,idProveedor),nuevoProducto.unidadNegocio)

    def crearVenta(self,venta):
        idVenta=self.buscarVentaRam(venta)
        if not idVenta:
            idPeriodo=self.buscarPeriodoRam(venta.periodo)
            idProducto=self.buscarProductoRam(venta.producto,venta.unidadNegocio)
            idRegistroVentaCreado=self.ventas.create({
                'x_studio_periodo':idPeriodo,
                'x_studio_mes':venta.mes,
                'x_studio_producto':idProducto,
                'x_studio_ventaprecencialjumbo':venta.ventaPrecencialJumbo,
                'x_studio_ventaecommercejumbo':venta.ventaEcommerceJumbo,
                'x_studio_ventaprecencialconv':venta.ventaPrecencialConv,
                'x_studio_ventaecommerceconv':venta.ventaEcommerceConv,
                'x_studio_ventaprecencialsisa':venta.ventaPrecencialSisa,
                'x_studio_ventaecommercesisa':venta.ventaEcommerceSisa,
                'x_studio_ventaprecencialeasy':venta.ventaPrecencialEasy,
                'x_studio_ventainterneteasy':venta.ventaInternetEasy,
                'x_studio_ventamayoristaeasy':venta.ventaMayoristaEasy,
                'x_studio_ventaprecencialparis':venta.ventaPrecencialParis,
            })
            if venta.unidadNegocio=='SMK':
                conjuntoVentas=self.conjuntoVentasSMK
                un=1
            elif venta.unidadNegocio=='MDH':
                conjuntoVentas=self.conjuntoVentasMDH
                un=2
            elif venta.unidadNegocio=='TXD':
                conjuntoVentas=self.conjuntoVentasTXD
                un=3
            conjuntoVentas.append({
                'id': idRegistroVentaCreado,
                'x_studio_periodo':[idPeriodo,venta.periodo],
                'x_studio_mes':venta.mes,
                'x_studio_producto': [self.buscarProductoRam(venta.producto,venta.unidadNegocio), str(venta.producto)],
                'x_studio_unidades_de_negocio':[un],
                'x_studio_ventaprecencialjumbo':venta.ventaPrecencialJumbo,
                'x_studio_ventaecommercejumbo':venta.ventaEcommerceJumbo,
                'x_studio_ventaprecencialconv':venta.ventaPrecencialConv,
                'x_studio_ventaecommerceconv':venta.ventaEcommerceConv,
                'x_studio_ventaprecencialsisa':venta.ventaPrecencialSisa,
                'x_studio_ventaecommercesisa':venta.ventaEcommerceSisa,
                'x_studio_ventaprecencialeasy':venta.ventaPrecencialEasy,
                'x_studio_ventainterneteasy':venta.ventaInternetEasy,
                'x_studio_ventamayoristaeasy':venta.ventaMayoristaEasy,
                'x_studio_ventaprecencialparis':venta.ventaPrecencialParis,            
            })      
    def crearEan(self,objEan,unidadNegocio):
        idSKU=objEan.idSku
        idProveedor=objEan.idProveedor
        idEAN=self.buscarEanRam(str(objEan.codEAN),str(self.buscarProductoRamId(idSKU,unidadNegocio))+unidadNegocio)
        if not idEAN:
            idEAN = self.ean.create({
                'x_name':str(objEan.codEAN),
                'x_studio_sku':idSKU,
                'x_studio_proveedor':idProveedor
            })
        self.conjuntoEan.append({'id': idEAN, 'x_name': str(objEan.codEAN), 'x_studio_sku': [idSKU, self.buscarProductoRamId(idSKU,unidadNegocio)], 'x_studio_proveedor': [idProveedor, self.buscarProveedorRamId(idProveedor)]})
        return idEAN
    def crearDepartamento(self,nombreDepartamento):
        idDepartamento=self.buscarCategoriaRam(self.conjuntoDepartamentos,nombreDepartamento)
        if not idDepartamento:           
            idDepartamento = self.categoriaDepartamento.create({
                'x_name':nombreDepartamento,
            })
        self.conjuntoDepartamentos.append({'id': idDepartamento, 'x_name': nombreDepartamento})
        return idDepartamento
    def crearSubDepartamento(self,nombreSubDepartamento):
        idSubDepartamento=self.buscarCategoriaRam(self.conjuntoSubDepartamentos,nombreSubDepartamento)
        if not idSubDepartamento:           
            idSubDepartamento = self.categoriaSubdepartamento.create({
                'x_name':nombreSubDepartamento,
            })
        self.conjuntoSubDepartamentos.append({'id': idSubDepartamento, 'x_name': nombreSubDepartamento})
        return idSubDepartamento
    def crearCategoria(self,nombreCategoria):
        idCategoria=self.buscarCategoriaRam(self.conjuntocategorias,nombreCategoria)
        if not idCategoria:           
            idCategoria = self.categoriaCategoria.create({
                'x_name':nombreCategoria,
            })
        self.conjuntocategorias.append({'id': idCategoria, 'x_name': nombreCategoria})
        return idCategoria
    def crearSeccion(self,nombreSeccion):
        idSeccion=self.buscarCategoriaRam(self.conjuntoSecciones,nombreSeccion)
        if not idSeccion:           
            idSeccion = self.categoriaSeccion.create({
                'x_name':nombreSeccion,
            })
        self.conjuntoSecciones.append({'id': idSeccion, 'x_name': nombreSeccion})
        return idSeccion
    def crearRubro(self,nombreRubro):
        idRubro=self.buscarCategoriaRam(self.conjuntoRubro,nombreRubro)
        if not idRubro:           
            idRubro = self.categoriaRubro.create({
                'x_name':nombreRubro,
            })
        self.conjuntoRubro.append({'id': idRubro, 'x_name': nombreRubro})
        return idRubro
    def crearSubRubro(self,nombreSubRubro):
        idSubRubro=self.buscarCategoriaRam(self.conjuntoSubRubro,nombreSubRubro)
        if not idSubRubro:           
            idSubRubro = self.categoriaRubro.create({
                'x_name':nombreSubRubro,
            })
        self.conjuntoSubRubro.append({'id': idSubRubro, 'x_name': nombreSubRubro})
        return idSubRubro
    def documentoGeneradoSIG(self,idRegistroSIG):
        ob=self.declaracionesSIG.search([('id','=',idRegistroSIG)])
        self.declaracionesSIG.write(ob,{
            'x_studio_doc_generado':1,
        })
    def documentoGeneradoGrupoEcotasa(self,idRegistro):
        ob=self.grupoEcotasa.search([('id','=',idRegistro)])
        self.grupoEcotasa.write(ob,{
            'x_studio_documento_generado':1,
        })

    def crearCategoria1(self,lista1,nombreCategoria1,unidadNegocio):
        idCAtegoria1=self.buscarCategoriaRam(lista1,nombreCategoria1)
        if not idCAtegoria1:
            if unidadNegocio=='SMK':            
                idCAtegoria1 = self.categoriaDepartamento.create({
                    'x_name':nombreCategoria1,
                })
            elif unidadNegocio=='MDH':            
                idCAtegoria1 = self.categoriaSeccion.create({
                    'x_name':nombreCategoria1,
                })
            elif unidadNegocio=='TXD':            
                idCAtegoria1 = self.categoriaDepartamento.create({
                    'x_name':nombreCategoria1,
                })
        lista1.append({'id': idCAtegoria1, 'x_name': nombreCategoria1})
        return idCAtegoria1 
    def crearCategoria2(self,lista2,nombreCategoria2,unidadNegocio):
        idCAtegoria2=self.buscarCategoriaRam(lista2,nombreCategoria2)
        if not idCAtegoria2:
            if unidadNegocio=='SMK':            
                idCAtegoria2 = self.categoriaSubdepartamento.create({
                    'x_name':nombreCategoria2,
                })
            elif unidadNegocio=='MDH':            
                idCAtegoria2 = self.categoriaRubro.create({
                    'x_name':nombreCategoria2,
                })
            elif unidadNegocio=='TXD':            
                idCAtegoria2 = self.categoriaSubdepartamento.create({
                    'x_name':nombreCategoria2,
                })
        lista2.append({'id': idCAtegoria2, 'x_name': nombreCategoria2})
        return idCAtegoria2 
    def crearCategoria3(self,lista3,nombreCategoria3,unidadNegocio):
        idCAtegoria3 =self.buscarCategoriaRam(lista3,nombreCategoria3)
        if not idCAtegoria3:
            if unidadNegocio=='SMK':            
                idCAtegoria3 = self.categoriaCategoria.create({
                    'x_name':nombreCategoria3,
                })
            elif unidadNegocio=='MDH':            
                idCAtegoria3 = self.categoriaSubrubro.create({
                    'x_name':nombreCategoria3,
                })
            elif unidadNegocio=='TXD':            
                idCAtegoria3 = self.categoriaCategoria.create({
                    'x_name':nombreCategoria3,
                })
        lista3.append({'id': idCAtegoria3, 'x_name': nombreCategoria3})
        return idCAtegoria3  
    def buscarSKU(arreglo,sku):
        arreglo=[]
        for producto in arreglo:
            if producto.sku==sku:
                return 1
        return 0     
    def buscarPrueba(self,cSku,dframe):
        for row in range(len(dframe)):
            if dframe.iloc[row]['Producto']==cSku and cSku:
                return dframe.iloc[row]
        return None           
    def prueba(self,df):
        df1 = df
        df2 = df

        df3 = df2.merge(df1, left_index=True, right_on="GRID", how='left')
        df3.reset_index(inplace=True, drop=True)





        for fila in range(len(df)):
            producto=df.iloc[fila]['Producto']
            prod = df.iloc[fila]['Producto/Todos los elementos/Producto']
            if not producto:
                filaMaster=self.buscarPrueba(prod,df)
                if filaMaster:
                    df.iloc[fila]['Producto/SKU Unidad de negocio']=filaMaster['Producto/SKU Unidad de negocio']
                    df.iloc[fila]['Producto/Departamento']=filaMaster['Producto/Departamento']
                    df.iloc[fila]['Producto/Rubro']=filaMaster['Producto/Rubro']
                    df.iloc[fila]['TOTAL CONVENIENCIA']=filaMaster['TOTAL CONVENIENCIA']
                    df.iloc[fila]['TOTAL JUMBO']=filaMaster['TOTAL JUMBO']
                    df.iloc[fila]['TOTAL SISA']=filaMaster['TOTAL SISA']
                    df.iloc[fila]['TOTAL EASY']=filaMaster['TOTAL EASY']
                    df.iloc[fila]['TOTAL PARIS']=filaMaster['TOTAL PARIS']
                    df.iloc[fila]['Producto/Etapa']=filaMaster['Producto/Etapa']
        return df

    def revisarDF_SMK(self,dataframe,tempo):
        
        if dataframe.tipo=='producto_SM':
            print('iniciando proceso productos SMK')
            fila=1
            df=dataframe.dataFrame
            largo=len(df)
            avance25=1
            avance50=1
            avance75=1
            for fila in range(len(df)):
                if tempo.verificarTiempoLimite(time.time()):
                    if fila>largo/4 and avance25:
                        print('Dataframe de productos procesado en un 25%')
                        avance25=0
                    elif fila>largo/2 and avance50:
                        print('Dataframe de productos procesado en un 50%')
                        avance50=0
                    elif fila>(largo*3)/4 and avance75:
                        print('Dataframe de productos procesado en un 75%')
                        avance75=0
                    #print(datoFila['Sku'])
                    skuProducto=df.loc[fila]['Sku']
                    eanProducto=df.loc[fila]['Ean']
                    busquedaSKU=self.buscarProductoRam(str(skuProducto),'SMK')
                    if not busquedaSKU:
                        prod= Producto(
                            'SMK',
                            skuProducto,
                            eanProducto,
                            df.loc[fila]['Cod_Refer_Proveedor'],
                            df.loc[fila]['Proveedor'],
                            df.loc[fila]['Descripcion'],
                            df.loc[fila]['Origen'],
                            df.loc[fila]['Tipo_Marca'],
                            'No completado - Nuevo',
                            df.loc[fila]['Departamento'],
                            df.loc[fila]['Categoria'],
                            df.loc[fila]['Sub_Categoria'],
                            df.loc[fila]['Medida_Producto'],
                            '',
                            '',
                            )
                        idSKU=self.crearProducto(prod)
                    elif not self.buscarEanRam(str(eanProducto),str(skuProducto)+'SMK'):
                        idProveedor=self.buscarProveedorRam(df.loc[fila]['Proveedor'])

                        if idProveedor:
                            objEan = Ean(str(df.loc[fila]['Ean']),str(busquedaSKU),str(idProveedor))
                        else:
                            idProveedorCreado=self.crearProveedor(Proveedor(df.loc[fila]['Proveedor'],df.loc[fila]['Cod_Refer_Proveedor'],''))
                            objEan = Ean(str(df.loc[fila]['Ean']),str(busquedaSKU),str(idProveedorCreado))
                        self.crearEan(objEan,'SMK')
                else:
                    return 0                
            print('Dataframe de productos procesado en un 100%')
            return 1
        elif dataframe.tipo=='venta_SM':
            print('iniciando proceso Venta SMK')
            fila=1
            df=dataframe.dataFrame
            largo=len(df)
            avance25=1
            avance50=1
            avance75=1
            for fila in range(len(df)):
                if tempo.verificarTiempoLimite(time.time()):    
                    if fila>largo/4 and avance25:
                        print('Dataframe de ventas procesado en un 25%')
                        avance25=0
                    elif fila>largo/2 and avance50:
                        print('Dataframe de ventas  procesado en un 50%')
                        avance50=0
                    elif fila>(largo*3)/4 and avance75:
                        print('Dataframe de ventas  procesado en un 75%')
                        avance75=0
                    presJumbo=0
                    ecoJumbo=0
                    presConveniencia=0
                    ecoConveniencia=0
                    ecoSisa=0
                    presSisa=0
                    if df.loc[fila]['Precencial_J']>0:
                        presJumbo=df.loc[fila]['Precencial_J']
                    if df.loc[fila]['Ecommerce_J']>0:
                        ecoJumbo=df.loc[fila]['Ecommerce_J']
                    if df.loc[fila]['Precencial_C']>0:
                        presConveniencia=df.loc[fila]['Precencial_C']
                    if df.loc[fila]['Ecommerce_C']>0:
                        ecoConveniencia=df.loc[fila]['Ecommerce_C']
                    if df.loc[fila]['Precencial_S']>0:
                        ecoSisa=df.loc[fila]['Precencial_S']
                    if df.loc[fila]['Ecommerce_S']>0:
                        presSisa=df.loc[fila]['Ecommerce_S']
                    if (presJumbo+ecoJumbo+presConveniencia+ecoConveniencia+ecoSisa+presSisa)>0:
                        #print(datoFila['Sku'])
                        skuProducto=df.loc[fila]['Item_Id']
                        busquedaSKU=self.buscarProductoRam(str(skuProducto),'SMK')
                        if not busquedaSKU:
                            prod= Producto(
                                'SMK',
                                str(skuProducto),
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                'No completado - Nuevo',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                )
                            #print(prod) 
                            self.crearProductoVentaSMK(prod)
                            yearVenta = procesarFecha(df.loc[fila]['MesProceso'])[0]
                            mesVenta = int(procesarFecha(df.loc[fila]['MesProceso'])[1])
                            ventaAcrear = Venta(
                                yearVenta, 
                                mesVenta, 
                                str(skuProducto),
                                'SMK',
                                presJumbo,
                                ecoJumbo,
                                presConveniencia,
                                ecoConveniencia,
                                presSisa,                            
                                ecoSisa,
                                0,
                                0,
                                0,
                                0,
                                0,
                                )
                            #print(self.buscarProductoRamId(ventaAcrear.idProducto,'SMK'))    
                            self.crearVenta(ventaAcrear)      
                        else:
                            yearVenta = procesarFecha(df.loc[fila]['MesProceso'])[0]
                            mesVenta = int(procesarFecha(df.loc[fila]['MesProceso'])[1])
                            ventaAcrear = Venta(
                                yearVenta, 
                                mesVenta, 
                                str(skuProducto),
                                'SMK',
                                presJumbo,
                                ecoJumbo,
                                presConveniencia,
                                ecoConveniencia,
                                presSisa,                            
                                ecoSisa,
                                0,
                                0,
                                0,
                                0,
                                0,
                                )
                            #print(self.buscarProductoRamId(ventaAcrear.idProducto,'SMK'))    
                            self.crearVenta(ventaAcrear)    
                else:
                    return 0
            print('Dataframe de ventas procesado en un 100%')
            return 1
        elif dataframe.tipo=='insumo_SM':
            print('iniciando proceso productos insumos SMK')
            fila=1
            df=dataframe.dataFrame
            largo=len(df)
            avance25=1
            avance50=1
            avance75=1
            print('iniciando proceso de insumos')
            for fila in range(len(df)):
                if tempo.verificarTiempoLimite(time.time()):
                    if fila>largo/4 and avance25:
                        print('Dataframe de insumos procesado en un 25%')
                        avance25=0
                    elif fila>largo/2 and avance50:
                        print('Dataframe de insumos procesado en un 50%')
                        avance50=0
                    elif fila>(largo*3)/4 and avance75:
                        print('Dataframe de insumos procesado en un 75%')
                        avance75=0
                    #print(datoFila['Sku'])
                    consumoJumbo=0
                    consumoSisa=0
                    consumoConveniencia=0
                    if df.loc[fila]['Consumo_Jumbo']>0:
                        consumoJumbo=df.loc[fila]['Consumo_Jumbo']
                    if df.loc[fila]['Consumo_SISA']>0:
                        consumoJumbo=df.loc[fila]['Consumo_SISA']
                    if df.loc[fila]['Consumo_SPID']>0:
                        consumoJumbo=df.loc[fila]['Consumo_SPID']
                    if (consumoJumbo+consumoSisa+consumoConveniencia)>0:
                        skuProducto=df.loc[fila]['Sku']
                        eanProducto=df.loc[fila]['Ean']
                        busquedaSKU=self.buscarProductoRam(str(skuProducto),'SMK')
                        if not busquedaSKU:
                            prod= Producto(
                                'SMK',
                                skuProducto,
                                eanProducto,
                                df.loc[fila]['Cod_Refer_Proveedor'],
                                df.loc[fila]['Proveedor'],
                                df.loc[fila]['Descripcion'],
                                df.loc[fila]['Origen'],
                                df.loc[fila]['Tipo_Marca'],
                                'No completado - Nuevo',
                                df.loc[fila]['Departamento'],
                                df.loc[fila]['Categoria'],
                                df.loc[fila]['Sub_Categoria'],
                                '',
                                '',
                                '',
                                )
                            idSKU=self.crearProductoInsumo(prod)
                            yearVenta = '2022'
                            mesVenta = 12
                            ventaAcrear = Venta(
                                yearVenta, 
                                mesVenta, 
                                str(skuProducto),
                                'SMK',
                                consumoJumbo,
                                0,
                                consumoConveniencia,
                                0,
                                consumoSisa,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                )
                            #print(self.buscarProductoRamId(ventaAcrear.idProducto,'SMK'))    
                            self.crearVenta(ventaAcrear)
                        elif not self.buscarEanRam(str(eanProducto),str(skuProducto)+'SMK'):
                            idProveedor=self.buscarProveedorRam(df.loc[fila]['Proveedor'])

                            if idProveedor:
                                objEan = Ean(str(df.loc[fila]['Ean']),str(busquedaSKU),str(idProveedor))
                            else:
                                idProveedorCreado=self.crearProveedor(Proveedor(df.loc[fila]['Proveedor'],df.loc[fila]['Cod_Refer_Proveedor'],''))
                                objEan = Ean(str(df.loc[fila]['Ean']),str(busquedaSKU),str(idProveedorCreado))
                            self.crearEan(objEan,'SMK')

            print('Dataframe de insumos procesado en un 100%')
            return 1
    def revisarDF_MDH(self,dataframe,tempo):
        if dataframe.tipo=='producto_MDH':
            print('iniciando proceso productos MDH')
            fila=1
            df=dataframe.dataFrame
            largo=len(df)
            avance25=1
            avance50=1
            avance75=1
            for fila in range(len(df)):
                if tempo.verificarTiempoLimite(time.time()):
                    if fila>largo/4 and avance25:
                        print('Dataframe procesado en un 25%')
                        avance25=0
                    elif fila>largo/2 and avance50:
                        print('Dataframe procesado en un 50%')
                        avance50=0
                    if fila>(largo*3)/4 and avance75:
                        print('Dataframe procesado en un 75%')
                        avance75=0
                    #print(datoFila['Sku'])
                    skuProducto=df.loc[fila]['Sku']
                    eanProducto=df.loc[fila]['Ean']
                    origen=df.loc[fila]['Origen']
                    if origen=='IMPORTADOS':
                        origen='IMPORTADO'
                    elif origen=='NACIONALES':
                        origen='NACIONAL'
                    busquedaSKU=self.buscarProductoRam(str(skuProducto),'MDH')
                    if not busquedaSKU:
                        prod= Producto(
                            'MDH',
                            skuProducto,
                            eanProducto,
                            df.loc[fila]['Cod_Refer_Proveedor'],
                            df.loc[fila]['Proveedor'],
                            df.loc[fila]['Descripcion'],
                            origen,
                            df.loc[fila]['Tipo_Marca'],
                            'No completado - Nuevo',
                            df.loc[fila]['Departamento'],
                            df.loc[fila]['Categoria'],
                            df.loc[fila]['Sub_Categoria'],
                            df.loc[fila]['Medida_Producto'],
                            '',
                            '',
                            )
                        idSKU=self.crearProducto(prod)
                    elif not self.buscarEanRam(str(eanProducto),str(skuProducto)+'MDH'):
                        idProveedor=self.buscarProveedorRam(df.loc[fila]['Proveedor'])

                        if idProveedor:
                            objEan = Ean(str(df.loc[fila]['Ean']),str(busquedaSKU),str(idProveedor))
                        else:
                            idProveedorCreado=self.crearProveedor(Proveedor(df.loc[fila]['Proveedor'],df.loc[fila]['Cod_Refer_Proveedor'],''))
                            objEan = Ean(str(df.loc[fila]['Ean']),str(busquedaSKU),str(idProveedorCreado))
                        self.crearEan(objEan,'MDH')
                else:
                    return 0
            print('Dataframe procesado en un 100%')
            return 1
        elif dataframe.tipo=='venta_MDH':
            print('iniciando proceso VENTA MDH')
            fila=1
            df=dataframe.dataFrame
            largo=len(df)
            avance25=1
            avance50=1
            avance75=1
            for fila in range(len(df)):
                if tempo.verificarTiempoLimite(time.time()):
                    if fila>largo/4 and avance25:
                        print('Dataframe procesado en un 25%')
                        avance25=0
                    elif fila>largo/2 and avance50:
                        print('Dataframe procesado en un 50%')
                        avance50=0
                    if fila>(largo*3)/4 and avance75:
                        print('Dataframe procesado en un 75%')
                        avance75=0
                    #print(datoFila['Sku'])
                    easyPresencial=0
                    easyInternet=0
                    easyMayorista=0
                    if df.loc[fila]['Cantidad_Presencial']>0:
                        easyPresencial=df.loc[fila]['Cantidad_Presencial']
                    if df.loc[fila]['Cantidad_Presencial']>0:
                        easyInternet=df.loc[fila]['Cantidad_Presencial']
                    if df.loc[fila]['Cantidad_Presencial']>0:
                        easyMayorista=df.loc[fila]['Cantidad_Presencial']
                    if (easyPresencial+easyInternet+easyMayorista)>0:
                        idSku=self.buscarProductoRam(str(df.loc[fila]['Articulo']),'MDH')
                        if not idSku:
                            origen = ''
                            if df.loc[fila]['Categoria_Valorizacion']=='Importado':
                                origen='IMPORTADO'
                            elif df.loc[fila]['Categoria_Valorizacion']=='Nacional':
                                origen='NACIONAL'
                            prod= Producto(
                                'MDH',
                                str(df.loc[fila]['Articulo']),
                                str(df.loc[fila]['Ean']),
                                df.loc[fila]['Cod_Proveedor_2'],
                                df.loc[fila]['Nombre_Proveedor'],
                                df.loc[fila]['Nombre_Articulo'],
                                origen,
                                df.loc[fila]['Nombre_Marca'],
                                'No completado - Nuevo',
                                df.loc[fila]['Nombre_Seccion'],
                                df.loc[fila]['Nombre_Rubro'],
                                df.loc[fila]['Nombre_Subrubro'],
                                )
                            idSku=self.crearProducto(prod)
                            venta=Venta(
                                str(df.loc[fila]['Ano']),
                                int(df.loc[fila]['Mes']),
                                str(df.loc[fila]['Articulo']),
                                'MDH',
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                easyPresencial,
                                easyInternet,
                                easyMayorista,
                                0,
                                0,
                            )
                            self.crearVenta(venta)
                        elif idSku:
                            venta=Venta(
                                str(df.loc[fila]['Ano']),
                                int(df.loc[fila]['Mes']),
                                str(df.loc[fila]['Articulo']),
                                'MDH',
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                easyPresencial,
                                easyInternet,
                                easyMayorista,
                                0,
                                0,
                            )
                            self.crearVenta(venta)
                else:
                    return 0
            print('Dataframe procesado en un 100%')
            return 1

    def revisarDF_TXD(self,dataframe,tempo):
        if dataframe.tipo=='producto_TXD':
            fila=1
            df=dataframe.dataFrame
            largo=len(df)
            avance25=1
            avance50=1
            avance75=1
            for fila in range(len(df)):
                if tempo.verificarTiempoLimite(time.time()):
                    if fila>largo/4 and avance25:
                        print('Dataframe procesado en un 25%')
                        avance25=0
                    elif fila>largo/2 and avance50:
                        print('Dataframe procesado en un 50%')
                        avance50=0
                    if fila>(largo*3)/4 and avance75:
                        print('Dataframe procesado en un 75%')
                        avance75=0
                
                    #print(datoFila['Sku'])
                    skuProducto=df.loc[fila]['ID JERARQUIA']
                    presParis = 0
                    ecoParis = 0
    
                    if df.loc[fila]['TIENDAS PRESCENCIALES'] != '-':
                        if int(float(df.loc[fila]['TIENDAS PRESCENCIALES'])) > 0:
                            presParis = int(float(df.loc[fila]['TIENDAS PRESCENCIALES']))
                    if df.loc[fila]['PARIS.CL'] != '-':        
                        if int(float(df.loc[fila]['PARIS.CL'])) > 0:
                            ecoParis = int(float(df.loc[fila]['PARIS.CL']))    
                    if(presParis+ecoParis)>0:
                        #eanProducto=df.loc[fila]['Ean']
                        busquedaSKU=self.buscarProductoRam(str(skuProducto),'TXD')
                        origen=''
                        if df.loc[fila]['ORIGIN TYPE']== 'NAC':
                            origen = 'NACIONAL'
                        elif df.loc[fila]['ORIGIN TYPE']== 'IMP':
                            origen = 'IMPORTADO'
                        if not busquedaSKU:
                            prod= Producto(
                                'TXD',
                                skuProducto,
                                '',
                                '',
                                '',
                                df.loc[fila]['CLASS DESC'],
                                origen,
                                df.loc[fila]['BRAND'],
                                'No completado - Nuevo',
                                df.loc[fila]['DEPT'],
                                df.loc[fila]['SUB DEPT'],
                                '',
                                '',
                                '',
                                '',
                                '',
                                )
                            idSku=self.crearProducto(prod)
                        ventaAcrear = Venta(
                            '2022',
                            12,
                            str(skuProducto),
                            'TXD',
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            presParis,
                            ecoParis
                        )
                    self.crearVenta(ventaAcrear)

                    #elif not self.buscarEanRam(str(eanProducto)):
                    #    idProveedor=self.buscarProveedorRam(df.loc[fila]['Proveedor'])

                    #    if idProveedor:
                    #        objEan = Ean(str(df.loc[fila]['Ean']),str(busquedaSKU),str(idProveedor))
                    #    else:
                    #        idProveedorCreado=self.crearProveedor(Proveedor(df.loc[fila]['Proveedor'],df.loc[fila]['Cod_Refer_Proveedor'],''))
                    #        objEan = Ean(str(df.loc[fila]['Ean']),str(busquedaSKU),str(idProveedorCreado))
                    #    self.crearEan(objEan,'MDH')
                else:
                    return 0
            print('Dataframe procesado en un 100%')
            return 1

    def procesarDF(self,dataFrame,aws,tempo):
        if dataFrame.unidadNegocio=='SMK':
            print('iniciando proceso SMK')
            if self.revisarDF_SMK(dataFrame,tempo)==1:
                aws.cambiarDocumentoFolder(dataFrame.nombrebucket,dataFrame.keyobj,'SMKPROCESADOS/','SMK')
                self.crearRegistroDF(RegistroDF(dataFrame.keyobj,1,dataFrame.tipo))
            return
        elif dataFrame.unidadNegocio=='MDH':
            print('iniciando proceso MDH')
            if self.revisarDF_MDH(dataFrame,tempo)==1:
                aws.cambiarDocumentoFolder(dataFrame.nombrebucket,dataFrame.keyobj,'MDHPROCESADOS/','MDH')
                self.crearRegistroDF(RegistroDF(dataFrame.keyobj,2,dataFrame.tipo))
            return
        elif dataFrame.unidadNegocio=='TXD':
            print('iniciando proceso TXD')
            if self.revisarDF_TXD(dataFrame,tempo)==1:
                aws.cambiarDocumentoFolder(dataFrame.nombrebucket,dataFrame.keyobj,'TXDPROCESADOS/','TXD')
                self.crearRegistroDF(RegistroDF(dataFrame.keyobj,3,dataFrame.tipo))
            return
        else:
            return

    def generarDFDeclaracionSIGMetodo2(self,razonSocial,periodo,temporizador,dataFrameAnterior):

        if razonSocial=='SISA':
            unidadNegocio='SMK'

        elif razonSocial=='JUMBO-CONVENIENCIA':
            unidadNegocio='SMK'

        elif razonSocial=='EASY':
            unidadNegocio='MDH'

        elif razonSocial=='PARIS':
            unidadNegocio='TXD'

        listaRegistrosVentas=self.traerVentasTotales(unidadNegocio,periodo)

        idPeriodo=self.buscarPeriodo(periodo)
        
        elementos=[]
        dfElementos=pd.DataFrame()
        dfNuevo=pd.DataFrame()
        if listaRegistrosVentas:
            for fila in range(len(listaRegistrosVentas)):
                if temporizador.verificarTiempoLimite(time.time()):
                    unidadNegocio=''
                    if razonSocial=='SISA':
                        unidadesVenta=listaRegistrosVentas[fila]['x_studio_total_sisa']
                    elif razonSocial=='JUMBO-CONVENIENCIA':
                        unidadesVenta=listaRegistrosVentas[fila]['x_studio_total_jumbo']+listaRegistrosVentas[fila]['x_studio_total_conveniencia']
                    elif razonSocial=='EASY':
                        unidadesVenta=listaRegistrosVentas[fila]['x_studio_total_easy']
                    elif razonSocial=='PARIS':
                        unidadesVenta=listaRegistrosVentas[fila]['x_studio_total_paris']
                    if unidadesVenta>0 and idPeriodo==listaRegistrosVentas[fila]['x_studio_periodo'][0]:
                        idSku=listaRegistrosVentas[fila]['x_studio_producto'][0]
                        if idSku:
                            if dataFrameAnterior!=None:
                                resp=self.buscarRegistroSig(dataFrameAnterior.dataFrame,idSku)
                            else:
                                resp = 0
                            if not resp:
                                partes=self.buscarMaterialidadesIDSku(idSku) 
                                if partes[0]:
                                    for parte in range(len(partes[0])):
                                        elemento1=partes[0][parte]
                                        elemento2=partes[1][parte]
                                        if elemento2['x_studio_categora']=='EYE Domiciliario' or elemento2['x_studio_categora']=='EYE No domiciliario':
                                            elementoAgregado=[elemento1['x_studio_producto'][0],
                                                            elemento1['x_studio_sku_unidad_de_negocio'],
                                                            elemento2['x_studio_descripcin_sku'],
                                                            elemento1['x_name'],
                                                            elemento1['x_studio_productos_por_envase'],
                                                            elemento1['x_studio_peso'],
                                                            elemento1['x_studio_peso_informado'],
                                                            elemento1['x_studio_mat'][1],
                                                            elemento2['x_studio_caractertica_del_material_solo_para_plsticos'],
                                                            elemento2['x_studio_definir_otro_material'],
                                                            elemento2['x_studio_caracterstica_reciclable'],
                                                            elemento2['x_studio_caracteristica_retornable'],
                                                            elemento2['x_studio_peligrosidad'],
                                                            elemento2['x_studio_categora'],
                                                            elemento2['x_studio_cat_material'],
                                                            unidadesVenta,
                                                            unidadesVenta*elemento1['x_studio_peso'],
                                                            (unidadesVenta*elemento1['x_studio_peso'])/1000,
                                                            (unidadesVenta*elemento1['x_studio_peso'])/1000000]
                                            
                                            '''elementoAgregado={'idSKU':elemento['x_studio_producto'][0],
                                                            'Código producto':elemento['x_studio_sku_unidad_de_negocio'],
                                                            'Descripción':elemento['x_studio_descripcin_sku'],
                                                            'Componentes':elemento['x_name'],
                                                            'Cantidades':elemento['x_studio_productos_por_envase'],
                                                            'Peso (g)':elemento['x_studio_peso'],
                                                            'Peso caja':elemento['x_studio_peso_informado'],
                                                            'Materiales':elemento['x_studio_mat'][1],
                                                            'Características':elemento['x_studio_caractertica_del_material_solo_para_plsticos'],
                                                            'Característica 1':elemento['x_studio_definir_otro_material'],
                                                            'Característica (material reciclado)':elemento['x_studio_caracterstica_reciclable'],
                                                            'Característica (retornabilidad)':elemento['x_studio_caracteristica_retornable'],
                                                            'Peligrosidad':elemento['x_studio_peligrosidad'],
                                                            'Categoría elemento':elemento['x_studio_categora'],
                                                            'Sub Categoria':elemento['x_studio_cat_material'],
                                                            'Total unidades vendidas':unidadesVenta,
                                                            'Total POM (g)':unidadesVenta*elemento['x_studio_peso'],
                                                            'Total POM (Kg)':(unidadesVenta*elemento['x_studio_peso'])/1000,
                                                            'Total POM (Ton)':(unidadesVenta*elemento['x_studio_peso'])/1000000,
                                                            }'''
                                            elementos.append(elementoAgregado)
                elif elementos:     
                    dfElementos=pd.DataFrame(elementos,columns=['idSKU',
                                                                'Código producto',
                                                                'Descripción',
                                                                'Componentes',
                                                                'Cantidades',
                                                                'Peso (g)',
                                                                'Peso caja',
                                                                'Materiales',
                                                                'Características',
                                                                'Característica 1',
                                                                'Característica (material reciclado)',
                                                                'Característica (retornabilidad)',
                                                                'Peligrosidad',
                                                                'Categoría elemento',
                                                                'Sub Categoria',
                                                                'Total unidades vendidas',
                                                                'Total POM (g)',
                                                                'Total POM (Kg)',
                                                                'Total POM (Ton)'
                                                                ])

                    if dataFrameAnterior:
                        dfNuevo=pd.concat([dataFrameAnterior.dataFrame,dfElementos])
                    else:
                        dfNuevo=dfElementos


                    return [dfNuevo,0]
            if elementos:
                dfElementos=pd.DataFrame(elementos,columns=['idSKU',
                                                            'Código producto',
                                                            'Descripción',
                                                            'Componentes',
                                                            'Cantidades',
                                                            'Peso (g)',
                                                            'Peso caja',
                                                            'Materiales',
                                                            'Características',
                                                            'Característica 1',
                                                            'Característica (material reciclado)',
                                                            'Característica (retornabilidad)',
                                                            'Peligrosidad',
                                                            'Categoría elemento',
                                                            'Sub Categoria',
                                                            'Total unidades vendidas',
                                                            'Total POM (g)',
                                                            'Total POM (Kg)',
                                                            'Total POM (Ton)'])
                if dataFrameAnterior:
                    dfNuevo=pd.concat([dataFrameAnterior.dataFrame,dfElementos])
                    return [dfNuevo,1]
                else:
                    dfNuevo=dfElementos
                    return [dfNuevo,1]
        else:
            return None

    def buscarRegistroSig(self,dataframe,idSku):
        if idSku in dataframe.idSKU.values:
            return 1
        '''for fila in range(len(dataframe)):
            if idSku==dataframe.iloc[fila]['idSKU']:
                return 1'''
        return 0

def getFechaString():
    fecha = datetime.datetime.now()
    fechaFormato = fecha.strftime("%Y")+fecha.strftime("%m")+fecha.strftime("%d")
    return fechaFormato

def buscarNombres(input,llave):
    output = []  
    for file in input:
        if file.find(llave)>1:
            output.append(file)
    return output

def cargarDataFrame(direccionDocumento,tipo):
    #hoja=pd.read_excel('TOTT05.6.01 Proveedores Faltantes Equipo DEC-TEX-HOG.xlsx',sheet_name=0)
    if tipo==1:
        df=pd.read_csv(direccionDocumento,header=0, sep='|', engine='python')
    elif tipo==2:
        df=pd.read_excel(direccionDocumento,sheet_name='ELEMENTOS EYE LEVANTADOS')
    return df

def procesarFecha(fecha):
    f = str(fecha)
    fechaSeparada = []
    fechaSeparada.append(f[:4])
    fechaSeparada.append(f[-2:])
    return(fechaSeparada)

def getDFProductos(dframes):
    for daf in dframes:
        tipo=daf.tipo
        var=tipo.find('producto')
        if var!=(-1):
            return daf
def getDFVentas(dframes):
    for df in dframes:
        if df.tipo.find('venta')!=(-1):
            return df
def getDFInsumo(dframes):
    for df in dframes:
        if df.tipo.find('insumo')!=(-1):
            return df
def getDFSIGincompletoSISA(dframes):
    for df in dframes:
        if df.tipo.find('SIG_SISA_SMK(NO_COMPLETADO).xlsx')!=(-1):
            return df
    return None
def getDFSIGincompletoJC(dframes):
    for df in dframes:
        valor=df.tipo.find('SIG_JUMBO-CONVENIENCIA_SMK(NO_COMPLETADO).xlsx')
        if valor!=(-1):
            return df
    return None
def getDFSIGincompletoMDH(dframes):
    for df in dframes:
        if df.tipo.find('SIG_MDH(NO_COMPLETADO).xlsx')!=(-1):
            return df
    return None
def getDFSIGincompletoTXD(dframes):
    for df in dframes:
        if df.tipo.find('SIG_TXD(NO_COMPLETADO).xlsx')!=(-1):
            return df
    return None

def lambda_handler(event,contex):
    a=Aws()
    temporizador=Temporizador(TIME_LIMIT)
    o=Odoo(ODOO_HOSTNAME,ODOO_DATABASE,ODOO_USERNAME,ODOO_PASSWORD)
    uNegocio="CORPORATIVO"
    dataframes=a.obtenerDataFrames(uNegocio,BUCKET_NAME,2)
    registrosSIG=o.conjutoDeclaracionesSIG
    for fila in range(len(registrosSIG)):
        idRegistro=registrosSIG[fila]['id']
        generarDetalle=registrosSIG[fila]['x_studio_generar_detalle_elementos']
        unidadNegocio=registrosSIG[fila]['x_studio_unidad_de_negocio'][1]
        periodo=registrosSIG[fila]['x_studio_periodo'][1]
        razonSocial=registrosSIG[fila]['x_studio_razon_social'][1]
        docGenerado=registrosSIG[fila]['x_studio_doc_generado']
        if not docGenerado and generarDetalle:
            if razonSocial=='SISA':
                dataf=getDFSIGincompletoSISA(dataframes)
                dfSIG= o.generarDFDeclaracionSIGMetodo2('SISA',periodo,temporizador,dataf)
                if dfSIG[1]==0:
                    dfSIG[0].to_excel('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_SISA_SMK(NO_COMPLETADO).xlsx',index=False,sheet_name='ELEMENTOS EYE LEVANTADOS')
                    a.upload_file('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_SISA_SMK(NO_COMPLETADO).xlsx','sftpasalvo-cencorep','CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_SISA_SMK(NO_COMPLETADO).xlsx')
                elif dfSIG[1]==1:
                    dfSIG[0].to_excel('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_SISA_SMK(COMPLETADO).xlsx',index=False,sheet_name='ELEMENTOS EYE LEVANTADOS')
                    a.upload_file('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_SISA_SMK(COMPLETADO).xlsx','sftpasalvo-cencorep','CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_SISA_SMK(COMPLETADO).xlsx')
                    a.s3_resource.meta.client.delete_object(
                        Bucket='sftpasalvo-cencorep',
                        Key='CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_SISA_SMK(NO_COMPLETADO).xlsx'
                    )
                    o.documentoGeneradoSIG(idRegistro)
                return
            elif razonSocial=='JUMBO + CONVENIENCIA':
                dataf=getDFSIGincompletoJC(dataframes)
                dfSIG= o.generarDFDeclaracionSIGMetodo2('JUMBO-CONVENIENCIA',periodo,temporizador,dataf)
                if dfSIG[1]==0:
                    dfSIG[0].to_excel('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_JUMBO-CONVENIENCIA_SMK(NO_COMPLETADO).xlsx',index=False,sheet_name='ELEMENTOS EYE LEVANTADOS')
                    a.upload_file('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_JUMBO-CONVENIENCIA_SMK(NO_COMPLETADO).xlsx','sftpasalvo-cencorep','CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_JUMBO-CONVENIENCIA_SMK(NO_COMPLETADO).xlsx')
                elif dfSIG[1]==1:
                    dfSIG[0].to_excel('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_JUMBO-CONVENIENCIA_SMK(COMPLETADO).xlsx',index=False,sheet_name='ELEMENTOS EYE LEVANTADOS')
                    a.upload_file('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_JUMBO-CONVENIENCIA_SMK(COMPLETADO).xlsx','sftpasalvo-cencorep','CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_JUMBO-CONVENIENCIA_SMK(COMPLETADO).xlsx')
                    a.s3_resource.meta.client.delete_object(
                        Bucket='sftpasalvo-cencorep',
                        Key='CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_JUMBO-CONVENIENCIA_SMK(NO_COMPLETADO).xlsx'
                    )
                    o.documentoGeneradoSIG(idRegistro)
                return
            elif razonSocial=='EASY':
                dataf=getDFSIGincompletoJC(dataframes)
                dfSIG= o.generarDFDeclaracionSIGMetodo2('EASY',periodo,temporizador,dataf)
                if dfSIG[1]==0:
                    dfSIG[0].to_excel('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_MDH(NO_COMPLETADO).xlsx',index=False,sheet_name='ELEMENTOS EYE LEVANTADOS')
                    a.upload_file('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_MDH(NO_COMPLETADO).xlsx','sftpasalvo-cencorep','CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_MDH(NO_COMPLETADO).xlsx')
                elif dfSIG[1]==1:
                    dfSIG[0].to_excel('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_MDH(COMPLETADO).xlsx',index=False,sheet_name='ELEMENTOS EYE LEVANTADOS')
                    a.upload_file('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_MDH(COMPLETADO).xlsx','sftpasalvo-cencorep','CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_MDH(COMPLETADO).xlsx')
                    a.s3_resource.meta.client.delete_object(
                        Bucket='sftpasalvo-cencorep',
                        Key='CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_MDH(NO_COMPLETADO).xlsx'
                    )
                    o.documentoGeneradoSIG(idRegistro)
                return
            elif razonSocial=='PARIS':
                dataf=getDFSIGincompletoTXD(dataframes)
                dfSIG= o.generarDFDeclaracionSIGMetodo2('PARIS',periodo,temporizador,dataf)
                if dfSIG[1]==0:
                    dfSIG[0].to_excel('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_TXD(NO_COMPLETADO).xlsx',index=False,sheet_name='ELEMENTOS EYE LEVANTADOS')
                    a.upload_file('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_TXD(NO_COMPLETADO).xlsx','sftpasalvo-cencorep','CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_TXD(NO_COMPLETADO).xlsx')
                elif dfSIG[1]==1:
                    dfSIG[0].to_excel('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_TXD(COMPLETADO).xlsx',index=False,sheet_name='ELEMENTOS EYE LEVANTADOS')
                    a.upload_file('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_TXD(COMPLETADO).xlsx','sftpasalvo-cencorep','CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_TXD(COMPLETADO).xlsx')
                    a.s3_resource.meta.client.delete_object(
                        Bucket='sftpasalvo-cencorep',
                        Key='CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_TXD(NO_COMPLETADO).xlsx'
                    )
                    o.documentoGeneradoSIG(idRegistro)
                return





