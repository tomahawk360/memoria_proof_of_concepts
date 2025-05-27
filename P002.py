import re
import json
import csv
import time
import xml
from ttp import ttp

#Fecha del log
file_name = '2024-09-20'

""" 
    ------- Observaciones -------
    En esta etapa se identifican los tiempos de inicio de las observaciones del dia correspondiente.

    Retorna: 
        - tpl_list: str[] -> Lista de tuplas, con el id de la observacion primero y el tiempo de inicio de la observacion segundo.
"""

tpl_list = []

with open("wdb_query_{0}.csv".format(file_name), mode="r") as query:
    reader = csv.reader(query)
    
    for line in reader:
        tpl_list.append((line[11], line[12].split("T")[1]))

tpl_list.pop(0)


""" 
    ------- Pre-procesado -------
    En esta etapa se identifican el header y las variables dinámicas triviales, eliminando el primero y
    marcando los segundos.

    Retorna: 
        - processed_lines: str[] -> Lista de lineas de log procesadas.
        - headless_lines: str[] -> Lista de tuplas (index, linea sin cabecera), pertenecientes a una observacion.
"""
headerless_lines = []
processed_lines = [] 

with open(file='logs/wt1tcs.{0}.log'.format(file_name), mode='r', encoding='iso-8859-1') as logs:

    lines = logs.readlines()
    extraction_flag = False

    for index, line in enumerate(lines):
        
        #Eliminación del header del log
        header_re = re.compile(r'([a-zA-Z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}?)\swt1tcs\s(.+)(\[[0-9]+\]):')

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

        # Logica de extraccion de linea de log
        if(extraction_flag): 
            headerless_lines[-1].append((index, headerless_line))

            #Eliminación de variables dinámicas triviales        
            datetime_re = re.compile(r'[0-2][0-9]{3}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*')
            date_re = re.compile(r'[0-2][0-9]{3}-[0-1][0-9]-[0-3][0-9]')
            time_re = re.compile(r'[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*')


            datetimeless_line = re.sub(datetime_re, '{{datetime}}', headerless_line)
            dateless_line = re.sub(date_re, '{{date}}', datetimeless_line)
            timeless_line = re.sub(time_re, '{{time}}', dateless_line)

            processed_lines.append(timeless_line)

""" 
    ------- Agrupamiento -------
    En esta etapa se delimitan los tokens de cada linea, se presumen los tokens estáticos como aquellos que solo posean letras
    y se agrupan las lineas según estos tokens estáticos.

    Retorna: 
        - grouped_lines: dict[] -> Lista de diccionarios, cada diccionario compuesto de dos campos:
            * s_t: Lista con tokens estáticos.
            * lines: Lista con los tokens de las lineas de log correspondientes al grupo.
"""
grouped_lines = []

for line in processed_lines:

    #Delimitar tokens
    tokens = line.split()

    #Identificar tokens estáticos
    static_tokens = [x for x in tokens if x.isalpha()]

    #Comparar tokens estáticos con otros grupos; guardar si hay match
    saved_flag = False

    for group in grouped_lines:
        if (group["s_t"] == static_tokens):
            group["lines"].append(tokens)
            saved_flag = True
            break

    #Si no hay match, crear y guardar en nuevo grupo
    if (not saved_flag):
        new_group = {
            "s_t" : static_tokens,
            "lines" : [tokens]
        }

        grouped_lines.append(new_group)

""" 
    ------- Análisis -------
    En esta etapa se cuenta la repetición de tokens por grupo y luego se dividen entre tokens estáticos y tokens dinámicos
    basandose en el número de veces que se repiten: aquellos que se repitan en todos los tokens del grupo se consideran estáticos y se
    añaden a una plantilla, y el resto de tokens se consideran dinámicos y se extraen como datos del grupo.

    Retorna:
        - templates: str[] -> Lista de las plantillas de cada grupo.
"""
templates = []

for group in grouped_lines:
    
    #Ordenar lineas del grupo segun tamaño de lista de forma descendiente
    group["lines"].sort(key=lambda x: len(x), reverse=True)

    #Obtener máximo de las lineas del grupo
    max_length = len(group["lines"][0])

    #Contar repeticiones de tokens
    token_counter = {}

    for tokens in group["lines"]:

        #Obtener duplicados
        duplicates = [x for x in set(tokens) if tokens.count(x) > 1]

        #Conteo
        for token in tokens:
            if(token in list(token_counter) and token not in duplicates):
                token_counter[token] += 1
            
            else:
                if(token not in duplicates):
                    token_counter[token] = 1
                else:
                    token_counter[token + '_duplicado'] = 1
                    duplicates.remove(token)

    #Selección de tokens para plantilla segun repeticiones
    template = ""

    #Obtener la máxima frecuencia de ocurrencia de tokens
    max_frecuency = len(group["lines"])

    #Contador de tokens dinámicos
    i_td = 0

    #Contador de tokens totales
    i_tt = 0

    #Generación de la plantilla
    while (i_tt < max_length):

        try:
            token = list(token_counter)[i_tt]

            if (token_counter[token] == max_frecuency):
                template = template + " " + token + " "

            else:
                template = template + " {{%d}} " % i_td
                i_td += 1

            i_tt += 1
        
        except IndexError as e:
            #print(template)
            break
    
    #Normalizar duplicados
    template_final = template.replace('_duplicado', '')

    #Guardar plantilla y datos
    templates.append(template_final)

""" 
    ------ Parsing -------
    Se parsea cada linea de log usando las plantillas generada anteriormente, usando TTP.
"""
parsed_data = []

i_time = time.time()

with open("P002_log.txt", "w") as p_log:
    #Marca de NUM TEMPLATES para log
    num_templates_log = "*** NUM TEMPLATES: " + str(len(templates)) + "\n"
    p_log.write(num_templates_log)
    print(num_templates_log)

    #Revisar que grupos de lineas de log calzan a una observacion
    for log_section in headerless_lines:

        parsing_flag = False

        stop_line = log_section[0][1]
        start_line = log_section[-1][1]
        
        time_re = re.compile(r'[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*')

        stop_time = re.search(time_re,stop_line).group()
        start_time = re.search(time_re,start_line).group()

        for tpl_id, tpl_time in tpl_list:

            print(tpl_id)
            print(tpl_time)
            print('---')

            if(stop_time < tpl_time and tpl_time < start_time):
                
                #Marca de TLP ID para log
                tpl_id_log = "*** TPL ID: " + tpl_id + "\n"
                p_log.write(tpl_id_log)
                print(tpl_id_log)

                #Habilitar parseo de seccion de log
                parsing_flag = True
                break

        #Parseo de la seccion de log
        if(parsing_flag):

            for index, line in log_section:

                #Marca de LINEA para log
                line_log = "*** LINEA: " + line + "\n"
                p_log.write(line_log)
                print(line_log)

                #Itera las 1300+ plantillas
                for template in templates:

                    #Parseo con expresiones regulares
                    try:
                        parser = ttp(data=line, template=template)
                        parser.parse()
                        key = list(parser.result(template='per_input', structure="dictionary"))[0]
                        result = parser.result(template='per_input', structure="dictionary")[key][0]

                        #Si hay match con la expresion regular, entonces se parsean los valores dinámicos y se salta a la siguiente linea
                        if(len(result) != 0):

                            #Marca de PLANTILLA para log
                            template_log = "PLANTILLA: " + template + "\n"
                            p_log.write(template_log)
                            print(template_log)

                            # Guardar como resultado
                            result['Num_linea'] = index
                            parsed_data.append(result)
                            break

                    #Trasciende de errores por etiquetas con mismo nombre en la plantilla (se conoce solución pero se posterga su implementación)
                    except re.PatternError as e:
                        continue

                    #Recolecta otros errores sobre la plantilla usada (se revisarán a posteriori)
                    except xml.etree.ElementTree.ParseError as err:                
                        error_log = "ERROR: " + str(err) + "\n"
                        p_log.write(error_log)
                        print(error_log)

f_time = time.time()

#Tiempo de SOLO parseo es de 3386.1755497455597 segundos (56.44 minutos)
print(f_time - i_time)

with open("log_plantillas.txt", "w") as data_file:
    json.dump(parsed_data, data_file)
    data_file.writelines(map(lambda x: x + "\n", templates))
