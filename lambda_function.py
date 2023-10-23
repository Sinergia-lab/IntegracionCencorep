
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
from OddoDownload import OdooDownloadCenco


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

    conn_params_cenco = {
        'ODOO_USERNAME' : ODOO_USERNAME,
        'ODOO_PASSWORD' : ODOO_PASSWORD,
        'ODOO_HOSTNAME' : ODOO_HOSTNAME,
        'ODOO_DATABASE' : ODOO_DATABASE
        }
    odoo_cenco = OdooDownloadCenco(conn_params_cenco)

    uNegocio="CORPORATIVO"
    dataframes=a.obtenerDataFrames(uNegocio,BUCKET_NAME,2)
    registrosSIG=o.conjutoDeclaracionesSIG
    for fila in range(len(registrosSIG)):
        idRegistro=registrosSIG.iloc[fila]['id']
        generarDetalle=registrosSIG.iloc[fila]['x_studio_generar_detalle_elementos'] =='True'
        unidadNegocio=eval(registrosSIG.iloc[fila]['x_studio_unidad_de_negocio'])[1]
        periodo=eval(registrosSIG.iloc[fila]['x_studio_periodo'])[1]
        razonSocial=eval(registrosSIG.iloc[fila]['x_studio_razon_social'])[1]
        docGenerado=registrosSIG.iloc[fila]['x_studio_doc_generado'] =='True'

        if not docGenerado and generarDetalle:
            if razonSocial=='SISA':
                odoo_cenco.declaracion_eye(periodo,'SISA',download=False)
                dfSIG = odoo_cenco.resultadoBusqueda
                dfSIG['Producto']=dfSIG['Producto'].str.replace('SMK/TXD','')
                dfSIG['Producto']=dfSIG['Producto'].str.replace('SMK','')
                dfSIG.to_excel('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_SISA_SMK(COMPLETADO).xlsx',index=False,sheet_name='ELEMENTOS EYE LEVANTADOS')
                a.upload_file('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_SISA_SMK(COMPLETADO).xlsx','sftpasalvo-cencorep','CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_SISA_SMK(COMPLETADO).xlsx')
                a.s3_resource.meta.client.delete_object(
                    Bucket='sftpasalvo-cencorep',
                    Key='CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_SISA_SMK(NO_COMPLETADO).xlsx'
                )
                odoo_cenco.documentoGeneradoSig(idRegistro)
                return
            elif razonSocial=='JUMBO + CONVENIENCIA':
                odoo_cenco.declaracion_eye(periodo,'JUMBO',download=False)
                dfSIG = odoo_cenco.resultadoBusqueda
                dfSIG['Producto']=dfSIG['Producto'].str.replace('SMK/TXD','')
                dfSIG['Producto']=dfSIG['Producto'].str.replace('SMK','')
                dfSIG.to_excel('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_JUMBO-CONVENIENCIA_SMK(COMPLETADO).xlsx',index=False,sheet_name='ELEMENTOS EYE LEVANTADOS')
                a.upload_file('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_JUMBO-CONVENIENCIA_SMK(COMPLETADO).xlsx','sftpasalvo-cencorep','CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_JUMBO-CONVENIENCIA_SMK(COMPLETADO).xlsx')
                a.s3_resource.meta.client.delete_object(
                    Bucket='sftpasalvo-cencorep',
                    Key='CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_JUMBO-CONVENIENCIA_SMK(NO_COMPLETADO).xlsx'
                )
                odoo_cenco.documentoGeneradoSig(idRegistro)
                return
            elif razonSocial=='EASY':
                odoo_cenco.declaracion_eye(periodo,'MDH',download=False)
                dfSIG = odoo_cenco.resultadoBusqueda
                dfSIG['Producto']=dfSIG['Producto'].str.replace('MDH','')
                dfSIG.to_excel('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_MDH(COMPLETADO).xlsx',index=False,sheet_name='ELEMENTOS EYE LEVANTADOS')
                a.upload_file('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_MDH(COMPLETADO).xlsx','sftpasalvo-cencorep','CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_MDH(COMPLETADO).xlsx')
                a.s3_resource.meta.client.delete_object(
                    Bucket='sftpasalvo-cencorep',
                    Key='CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_MDH(NO_COMPLETADO).xlsx'
                )
                odoo_cenco.documentoGeneradoSig(idRegistro)
                return
            elif razonSocial=='PARIS':
                odoo_cenco.declaracion_eye(periodo,'TXD',download=False)
                dfSIG = odoo_cenco.resultadoBusqueda
                dfSIG['Producto']=dfSIG['Producto'].str.replace('SMK/TXD','')
                dfSIG['Producto']=dfSIG['Producto'].str.replace('TXD','')
                dfSIG.to_excel('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_TXD(COMPLETADO).xlsx',index=False,sheet_name='ELEMENTOS EYE LEVANTADOS')
                a.upload_file('/tmp/'+getFechaString()+'_LeyRep_declaracion_SIG_TXD(COMPLETADO).xlsx','sftpasalvo-cencorep','CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_TXD(COMPLETADO).xlsx')
                a.s3_resource.meta.client.delete_object(
                    Bucket='sftpasalvo-cencorep',
                    Key='CORPORATIVO/'+getFechaString()+'_LeyRep_declaracion_SIG_TXD(NO_COMPLETADO).xlsx'
                )
                odoo_cenco.documentoGeneradoSig(idRegistro)
                return





