import re
import json
import csv
import time
import xml
import logging
import tracemalloc
from ttp import ttp


""" 
    ------- Observaciones -------
    En esta etapa se identifican los tiempos de inicio de las observaciones del dia correspondiente.

    Retorna: 
        - tpl_list: str[] -> Lista de tuplas, con el id de la observacion primero y el tiempo de inicio de la observacion segundo.
"""

def open_obs_file(arch_name):
    tpl_list = []

    with open(arch_name, mode="r") as query:
        reader = csv.reader(query)
        
        for line in reader:
            tpl_list.append((line[11], line[12].split("T")[1]))

    tpl_list.pop(0)

    return tpl_list


def open_txt_file(arch_name):
    with open(file=arch_name, mode='r', encoding='iso-8859-1') as logs:
        lines = logs.readlines()

    return lines


""" 
    Nombre:   
        header_removal
    Retorna: 
        headless_lines: -> Lista de listas de tuplas (index, linea sin cabecera), con cada lista correspondiente a las lineas
        entre el fin de un ciclo de autoguider del VLT y el inicio del siguiente ciclo de autoguider.
"""
def header_removal(logger, log_lines):
    headerless_lines = []
    extraction_flag = False

    for index, line in enumerate(log_lines):
        
        #Eliminación del header del log
        header_re = re.compile(r'([a-zA-Z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}?)\swt1tcs\s(.+)(\[[0-9]+\]):\s')

        headerless_line = re.sub(header_re, '', line)

        if(headerless_line.find("AG.GUIDE") != -1):

            if(headerless_line.find("STOP") != -1):
                # Comenzar extraccion de lineas de log
                headerless_lines.append([])
                extraction_flag = True

            elif(headerless_line.find("START") != -1):
                # Terminar extraccion de lineas de log
                if(len(headerless_lines) != 0):
                    headerless_lines[-1].append((index, headerless_line))

                extraction_flag = False

        if(extraction_flag): 
            headerless_lines[-1].append((index, headerless_line))

    return headerless_lines


""" 
    Nombre:   
        obs_filtering
    Retorna: 
        obs_log_sections: -> Lista de listas de tuplas (index, linea sin cabecera), cada lista perteneciente a una observacion.
"""
def obs_filtering(logger, log_lines, obs_list):
    obs_log_sections = []

    #Revisar que grupos de lineas de log calzan a una observacion
    for log_section in log_lines:

        stop_line = log_section[0][1]
        start_line = log_section[-1][1]
        
        time_re = re.compile(r'[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*')

        stop_time = re.search(time_re,stop_line).group()
        start_time = re.search(time_re,start_line).group()

        for tpl_id, tpl_time in obs_list:

            if(stop_time < tpl_time and tpl_time < start_time):
                logger.info("*** TPL ID: " + tpl_id)

                #Habilitar parseo de seccion de log
                obs_log_sections.append(log_section)
                break

    return obs_log_sections


""" 
    Nombre:   
        log_parsing
    Retorna: 
        parsed_data: -> Lista de jsons con los datos extraidos de las lineas de log.
"""
def log_parsing(logger, log_sections, templates_list):
    parsed_data = []

    for log_section in log_sections:
        for index, line in log_section:

            #Marca de LINEA para log
            logger.info("*** LINEA: " + line)

            #Itera las 14 plantillas
            for template in templates_list:

                #Parseo con expresiones regulares
                try:
                    parser = ttp(data=line, template=template)
                    parser.parse()
                    key = list(parser.result(template='per_input', structure="dictionary"))[0]
                    result = parser.result(template='per_input', structure="dictionary")[key][0]

                    #Si hay match con la expresion regular, entonces se parsean los valores dinámicos y se salta a la siguiente linea
                    if(len(result) != 0):

                        logger.info("PLANTILLA: " + template)

                        # Guardar como resultado
                        result['Num_linea'] = index
                        parsed_data.append(result)
                        break

                #Recolecta otros errores sobre la plantilla usada (se revisarán a posteriori)
                except xml.etree.ElementTree.ParseError as err:
                    logger.info("ERROR: " + str(err))

    return parsed_data


def save_parsed_data(parsed_data, dest_arch_name):
    with open(dest_arch_name, "w") as data_file:
        json.dump(parsed_data, data_file)


#Main
if __name__ == '__main__':
    #Logger del MVP
    logging.basicConfig(filename='P004.log', level=logging.INFO)
    logger = logging.getLogger('myLogger')

    #Fecha del log
    file_name = '2024-09-20'

    #Nombre de archivo de destino para datos parseados
    dest_arch_name = 'P004_data.txt'

    #Csv de observaciones
    obs_arch_name = "wdb_query_{0}.csv".format(file_name)
    obs_list = open_obs_file(obs_arch_name)

    #Lineas de log
    log_arch_name = 'logs/wt1tcs.{0}.log'.format(file_name)
    log_lines = open_txt_file(log_arch_name)

    #Plantillas
    tplt_arch_name = 'P004_templates.txt'
    tplt_list = open_txt_file(tplt_arch_name)

    #Iniciar monitoreo de memoria 
    # tracemalloc.start()
    # init_malloc = tracemalloc.get_traced_memory()

    #Remocion de los headers
    i_time_step_1 = time.time()
    # i_malloc_1 = tracemalloc.get_traced_memory()

    headerless_logs = header_removal(logger, log_lines)

    # f_malloc_1 = tracemalloc.get_traced_memory()
    f_time_step_1 = time.time()

    time_step_1 = f_time_step_1 - i_time_step_1
    # malloc_1 = tuple(map(lambda x, y: x-y, f_malloc_1, i_malloc_1))

    logger.info('Remocion de headers: {0}'.format(str(time_step_1)))

    #Filtro por logs de observaciones
    i_time_step_2 = time.time()
    # i_malloc_2 = tracemalloc.get_traced_memory()

    obs_logs = obs_filtering(logger, headerless_logs, obs_list)

    # f_malloc_2 = tracemalloc.get_traced_memory()
    f_time_step_2 = time.time()

    time_step_2 = f_time_step_2 - i_time_step_2
    # malloc_2 = tuple(map(lambda x, y: x-y, f_malloc_2, i_malloc_2))

    logger.info('Filtro de lineas de observacion: {0}'.format(str(time_step_2)))

    #Parsear data de los logs
    i_time_step_3 = time.time()
    # i_malloc_3 = tracemalloc.get_traced_memory()

    parsed_data = log_parsing(logger, obs_logs, tplt_list)

    # f_malloc_3 = tracemalloc.get_traced_memory()
    f_time_step_3 = time.time()

    time_step_3 = f_time_step_3 - i_time_step_3
    # malloc_3 = tuple(map(lambda x, y: x-y, f_malloc_3, i_malloc_3))

    logger.info('Parseo de informacion: {0}'.format(str(time_step_3)))

    #Termino de monitoreo de memoria
    # tracemalloc.stop()

    #Guardar data parseada
    save_parsed_data(parsed_data, dest_arch_name)

