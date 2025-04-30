import re

#Fecha del log
file_name = '2024-09-20'


""" 
    ------- Pre-procesado -------
    En esta etapa se identifican el header y las variables dinámicas triviales, eliminando el primero y
    marcando los segundos.

    Retorna: 
        - processed_lines: str[] -> Lista de lineas de log procesadas.
"""
processed_lines = [] 

with open(file='logs/wt1tcs.{0}.log'.format(file_name), mode='r', encoding='iso-8859-1') as logs:

    lines = logs.readlines()

    for line in lines:
        
        #Eliminación del header del log
        header_re = re.compile(r'([a-zA-Z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}?)\swt1tcs\s(.+)(\[[0-9]+\]):')

        headerless_line = re.sub(header_re, '', line)

        #Eliminación de variables dinámicas triviales
        date_re = re.compile(r'[0-2][0-9]{3}-[0-1][0-9]-[0-3][0-9]')
        time_re = re.compile(r'[0-2][0-9]:[0-5][0-9]:[0-5][0-9]')

        dateless_line = re.sub(date_re, '{{date}}', headerless_line)
        timeless_line = re.sub(time_re, '{{time}}', dateless_line)

        processed_lines.append(timeless_line)


""" 
    ------- Agrupamiento -------
    En esta etapa se delimitan los tokens de cada linea, se presumen los tokens estáticos como aquellos que solo posean letras
    y se agrupan las lineas según estos tokens estáticos.

    Retorna: 
        - grouped_lines: dict[] -> Lista de diccionarios, cada diccionario compuesto de dos campos:
            * s_t: Lista con tokens estáticos.
            * lines: Lista con las lineas de log correspondientes al grupo.
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
            group["lines"].append(line)
            saved_flag = True
            break

    #Si no hay match, crear y guardar en nuevo grupo
    if (not saved_flag):
        new_group = {
            "s_t" : static_tokens,
            "lines" : [line]
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

    #Contar repeticiones de tokens
    token_counter = {}

    for line in group["lines"]:
        
        #Delimitar tokens
        tokens = line.split()

        #Conteo
        for token in tokens:
            token_counter[token] += 1

    #Selección de tokens para plantilla segun repeticiones
    template = ""

    max_frecuency = len(group["lines"])
    i = 0

    for token in token_counter.keys():
        if (token_counter[token] == max_frecuency):
            template = template + " " + token + " "
        else:
            template = template + " {{ {0} }} ".format(i)
            i += 1

    #Guardar plantilla y datos
    templates.append(template)


""" 
    ------- Parsing -------
    Se parsea cada linea de log usando las plantillas generada anteriormente

"""