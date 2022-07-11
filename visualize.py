# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from dash import Dash, html, dcc, callback_context
import plotly.express as px
import pandas as pd
import mysql.connector
from datetime import datetime
from scrapy.utils.project import get_project_settings
import pymysql
import dash_split_pane
from dash.exceptions import PreventUpdate
from dash.dependencies import Output, Input, State
import plotly.graph_objs as go
import math


def generateTable(input):
    header=input[0]
    data=input[1]
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in header])),
        html.Tbody([
            html.Tr([
                html.Td(str(data[i][col])) for col in range(len(header))
            ]) for i in range(len(data))
        ])
    ])


settings = get_project_settings()
settings['MYSQL_DB'] = "one_week"


def ShowDomain(cursor, domain, page_limit):
    sql_sel_rel = "SELECT COUNT(page_url) as count, AVG(score) as average FROM Page " \
              "WHERE domain = %s AND score > 0.6"
    sql_sel_all = "SELECT COUNT(page_url) as count, AVG(score) as average FROM Page " \
              "WHERE domain = %s"
    sql_sel_pages = "SELECT title, page_url, score, found_hate_texts, time FROM Page " \
              "WHERE domain = %s ORDER BY score DESC LIMIT %s"
    cursor.execute(sql_sel_rel, domain['domain'])
    relevant = cursor.fetchone()
    if relevant is None:
        relevant['count'] == 0
        relevant['average'] = 0.0
    if relevant['count'] == 0:
        relevant['average'] = 0.0

    cursor.execute(sql_sel_all, domain['domain'])
    all = cursor.fetchone()
    if all is None:
        all['count'] == 0
        all['average'] = 0.0
    if all['count'] == 0:
        all['average'] = 0.0
    cursor.execute(sql_sel_pages, (domain['domain'], page_limit))
    pages = cursor.fetchall()
    rowspan = min(len(pages), page_limit)
    return html.Div(children=[
            html.H3(children=domain['domain']),
            html.Span(children="Páginas relevantes: " + str(relevant['count'])),
            html.Span(children="Páginas relevantes: " + str(domain['relevant_count'])),
            html.Span(children="Media punt. relevantes: " + str(round(relevant['average'], 4))),
            html.Span(children="Páginas totales: " + str(all['count'])),
            html.Span(children="Media punt. total: " + str(round(all['average'], 4))),
            html.Span(children="Descubrimiento: " + str(domain['discovered'])),
            html.Span(children="Última visita: " + str(domain['last_seen']))] + \
            [html.Div(children = [
                html.H4(children=page['title']),
                html.Span(children="Punt: " + str(round(page['score'], 4))),
                html.Span(children="Textos relevantes: " + str(page['found_hate_texts'])),
                html.Span(children="Visitado: " + str(page['time'])),
                html.P(children=page['page_url'])
                ], className="page-div-short") for page in pages[0:rowspan]
            ], className="domain-div-long")

def ShowPage(cursor, page, text_limit):
    sql_sel_texts = "SELECT DISTINCT(text), score FROM Text " \
              "WHERE page_url = %s ORDER BY score DESC LIMIT %s"
    cursor.execute(sql_sel_texts, (page['page_url'], text_limit))
    texts = cursor.fetchall()
    rowspan = min(len(texts), text_limit)
    return html.Div(children=[
            html.H3(children=page['title']),
                html.Span(children="Punt: " + str(round(page['score'], 4))),
                html.Span(children="Textos relevantes: " + str(page['found_hate_texts'])),
                html.Span(children="Visitado: " + str(page['time'])),
                html.P(children=page['page_url'])] + \
                [html.Div(children = [
                    html.H4(children="Score: " + str(round(text['score'], 4))),
                    html.P(children=text['text'])
                    ], className="text-div-short") for text in texts[0:rowspan]
            ], className="page-div-long"
        )

def ShowStats():
    return

def getInstanceName():
    return "Instance: " + settings['MYSQL_DB']


app = Dash(__name__)


app.layout = html.Div(children=[
    dcc.Store(id='domain_filters', storage_type='session'),
    dcc.Store(id='page_filters', storage_type='session'),
    dcc.Store(id='stats_filters', storage_type='session'),
    dcc.Store(id='results_filters', storage_type='session'),
    dcc.Store(id='last_session', storage_type='local'),
    dcc.Input(id='dummy',
        type="text",
        style={'display': 'none'}),
        html.Div(children=[
            html.H1(children=settings['BOT_NAME']),
            html.H2(children=getInstanceName()),
            html.H2(children=[], id="session_div"),
            ], className="title_div"),
        html.Div(children=[
            dcc.Tabs(children=[
                dcc.Tab(
                    label="Novedades",
                    children=[
                        html.Div(
                                id="novedades_div")
                        ], className="custom-tab",selected_className='custom-tab--selected'
                    ),
                dcc.Tab(
                    label="Dominios",
                    children=[
                        dcc.Input(
                            id="input_domain",
                            type="text",
                            placeholder="Dominio a buscar",
                            debounce=True,
                            className="input"
                        ),
                        dcc.Input(
                            id="input_min_pages",
                            type="number",
                            placeholder="Minimo X páginas relevantes",
                            debounce=True,
                            className="input"
                        ),
                        dcc.Input(
                            id="input_start_date_domain",
                            type="text",
                            placeholder="Descubierto desde: YYYY-MM-DD HH:MM:SS",
                            debounce=True,
                            pattern="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d",
                            className="input"
                        ),
                        dcc.Input(
                            id="input_end_date_domain",
                            type="text",
                            placeholder="Descubierto hasta: YYYY-MM-DD HH:MM:SS",
                            debounce=True,
                            pattern="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d",
                            className="input"
                        ),
                        dcc.Input(
                            id="show_pages",
                            type="number",
                            placeholder="Mostrar X páginas por dominio",
                            debounce=True,
                            className="input"
                        ),
                        html.Button('Limpiar', id='limpiar_domain', n_clicks=0),
                        html.Div(
                                id="domain_div")
                        ], className="custom-tab",selected_className='custom-tab--selected'
                    ),
                dcc.Tab(
                    label="Páginas",
                    children=[
                        dcc.Input(
                            id="input_page",
                            type="text",
                            placeholder="Página a buscar",
                            debounce=True,
                            className="input"
                        ),
                        dcc.Input(
                            id="input_min_score",
                            type="number",
                            placeholder="Minimo X score",
                            debounce=True,
                            className="input"
                        ),
                        dcc.Input(
                            id="input_start_date_page",
                            type="text",
                            placeholder="De: YYYY-MM-DD HH:MM:SS",
                            debounce=True,
                            pattern="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d",
                            className="input"
                        ),
                        dcc.Input(
                            id="input_end_date_page",
                            type="text",
                            placeholder="Hasta: YYYY-MM-DD HH:MM:SS",
                            debounce=True,
                            pattern="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d",
                            className="input"
                        ),
                        dcc.Input(
                            id="show_texts",
                            type="number",
                            placeholder="Mostrar X textos por página",
                            debounce=True,
                            className="input"
                        ),
                        html.Button('Limpiar', id='limpiar_page', n_clicks=0),
                        html.Div(
                                id="page_div")
                        ], className="custom-tab",selected_className='custom-tab--selected'
                    ),
                dcc.Tab(
                    label="Estadisticas de ejecución",
                    children=[
                        dcc.Input(
                            id="input_start_date_stats",
                            type="text",
                            placeholder="De: YYYY-MM-DD HH:MM:SS",
                            debounce=True,
                            pattern="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d",
                            className="input"
                        ),
                        dcc.Input(
                            id="input_end_date_stats",
                            type="text",
                            placeholder="Hasta: YYYY-MM-DD HH:MM:SS",
                            debounce=True,
                            pattern="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d",
                            className="input"
                        ),
                        html.Button('Limpiar', id='limpiar_stats', n_clicks=0),
                        html.Div(
                                id="stats_div")
                        ], className="custom-tab",selected_className='custom-tab--selected'
                    ),
                dcc.Tab(
                    label="Estadísticas de resultados",
                    children=[
                        dcc.Input(
                            id="input_end_date_results",
                            type="text",
                            placeholder="Hasta: YYYY-MM-DD HH:MM:SS",
                            debounce=True,
                            pattern="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d",
                            className="input"
                        ),
                        html.Button('Limpiar', id='limpiar_results', n_clicks=0),
                        html.Div(
                                id="results_div")
                        ], className="custom-tab",selected_className='custom-tab--selected'
                    )]
                , parent_className="custom-tabs"
                , className='custom-tabs-container'
                )
            ]
        )
]
)
# add a click to the appropriate store.
@app.callback(Output('last_session', 'data'),
              Input('dummy', 'value'),
              State('last_session', 'data'))
def StoreSessionFilters(dummy, last_session):
    last_session_new = {}
    if last_session:
        last_session_new['last_session'] = last_session['now_session']
        last_session_new['now_session'] = datetime.now()
    else:
        last_session_new['last_session'] = datetime(2022,1,1)
        last_session_new['now_session'] = datetime.now()
    return last_session_new

@app.callback(Output('novedades_div', 'children'),
              Input('last_session', 'modified_timestamp'),
              State('last_session', 'data'))
def NovedadesRefresh(ts, session):
    return []



@app.callback(Output('input_domain', 'value'),
              Output('input_min_pages', 'value'),
              Output('input_start_date_domain', 'value'),
              Output('input_end_date_domain', 'value'),
              Output('show_pages', 'value'),
              Input('limpiar_domain', 'n_clicks'))
def CleanDomainFilters(limpiar):
    filters = {}
    return None, None, None, None, None

# add a click to the appropriate store.
@app.callback(Output('input_page', 'value'),
              Output('input_min_score', 'value'),
              Output('input_start_date_page', 'value'),
              Output('input_end_date_page', 'value'),
              Output('show_texts', 'value'),
              Input('limpiar_page', 'n_clicks'))
def CleanPageFilters(limpiar):
    filters = {}
    return None, None, None, None, None

# add a click to the appropriate store.
@app.callback(Output('input_start_date_stats', 'value'),
              Output('input_end_date_stats', 'value'),
              Input('limpiar_stats', 'n_clicks'))
def CleanStatsFilters(limpiar):
    filters = {}
    return None, None

# add a click to the appropriate store.
@app.callback(Output('input_end_date_results', 'value'),
              Input('limpiar_results', 'n_clicks'))
def CleanResultsFilters(limpiar):
    filters = {}
    return None


# add a click to the appropriate store.
@app.callback(Output('domain_filters', 'data'),
              Input('input_domain', 'value'),
              Input('input_min_pages', 'value'),
              Input('input_start_date_domain', 'value'),
              Input('input_end_date_domain', 'value'),
              Input('show_pages', 'value'),
              State('domain_filters', 'data'))
def StoreDomainFilters(input_domain, input_min_pages, input_start_date, input_end_date, show_pages, filters):
    filters = {}
    if input_domain:
        filters['domain'] = input_domain
    if input_min_pages:
        filters['min_pages'] = input_min_pages
    if show_pages:
        filters['show_pages'] = show_pages
    if input_start_date:
        filters['start_date'] = input_start_date
    if input_end_date:
        filters['end_date'] = input_end_date
    return filters

@app.callback(Output('domain_div', 'children'),
              Input('domain_filters', 'modified_timestamp'),
              State('domain_filters', 'data'))
def DomainRefresh(ts, domain_filters):
    mydb = pymysql.connect(host=settings['MYSQL_HOST'],
                           user=settings['MYSQL_USER'],
                           password=settings['MYSQL_PASSWORD'],
                           database=settings['MYSQL_DB'],
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
    if ts is None:
        raise PreventUpdate
    page_limit = 5
    if domain_filters:
        filters = []
        if 'domain' in domain_filters:
            filters.append(" domain LIKE \"%%{}%%\" ".format(domain_filters['domain']))
        if 'min_pages' in domain_filters:
            filters.append(" found_hate_pages >= {} ".format(domain_filters['min_pages']))
        if 'start_date' in domain_filters:
            filters.append(" discovered_time > \"{}\" ".format(domain_filters['start_date']))
        if 'end_date' in domain_filters:
            filters.append(" discovered_time < \"{}\" ".format(domain_filters['end_date']))
        if len(filters) == 1:
             where_sql = "WHERE" + filters[0]
        elif len(filters) > 1:
             where_sql = "WHERE" + filters[0]
             for filter in filters[1:]:
                 where_sql = where_sql + "AND" + filter
        else:
            where_sql = ""
        if 'show_pages' in domain_filters:
            page_limit = domain_filters['show_pages']
    else:
        where_sql = ""
        
    sql_sel = "SELECT domain as domain, found_hate_pages as relevant_count, discovered_time as discovered, last_seen_time as last_seen FROM Domain " + where_sql + \
              "ORDER BY found_hate_pages DESC LIMIT %s"
    cursor = mydb.cursor()
    cursor.execute(sql_sel, 20)
    domains = cursor.fetchall()
    return_list = [ShowDomain(cursor, domain, page_limit) for domain in domains]
    cursor.close()
    mydb.close()
    return return_list


# add a click to the appropriate store.
@app.callback(Output('page_filters', 'data'),
              Input('input_page', 'value'),
              Input('input_min_score', 'value'),
              Input('input_start_date_page', 'value'),
              Input('input_end_date_page', 'value'),
              Input('show_texts', 'value'),
              State('page_filters', 'data'))
def StorePageFilters(input_page, input_min_score, input_start_date, input_end_date, show_texts, filters):
    filters = {}
    if input_page:
        filters['page'] = input_page
    if input_min_score:
        filters['min_score'] = input_min_score
    if input_start_date:
        filters['start_date'] = input_start_date
    if input_end_date:
        filters['end_date'] = input_end_date
    if show_texts:
        filters['show_texts'] = show_texts
    return filters


@app.callback(Output('page_div', 'children'),
              Input('page_filters', 'modified_timestamp'),
              State('page_filters', 'data'))
def PageRefresh(ts, page_filters):
    mydb = pymysql.connect(host=settings['MYSQL_HOST'],
                           user=settings['MYSQL_USER'],
                           password=settings['MYSQL_PASSWORD'],
                           database=settings['MYSQL_DB'],
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
    if ts is None:
        raise PreventUpdate
    text_limit = 10
    if page_filters:
        filters = []
        if 'page' in page_filters:
            filters.append(" page_url LIKE \"%%{}%%\" ".format(page_filters['page']))
        if 'min_score' in page_filters:
            filters.append(" score > {} ".format(page_filters['min_score']))
        if 'start_date' in page_filters:
            filters.append(" time > \"{}\" ".format(page_filters['start_date']))
        if 'end_date' in page_filters:
            filters.append(" time < \"{}\" ".format(page_filters['end_date']))
        if len(filters) == 1:
             where_sql = "WHERE" + filters[0]
        elif len(filters) > 1:
             where_sql = "WHERE" + filters[0]
             for filter in filters[1:]:
                 where_sql = where_sql + "AND" + filter
        else:
            where_sql = ""
        if 'show_texts' in page_filters:
            text_limit = page_filters['show_texts']
    else:
        where_sql = ""
        
    sql_sel = "SELECT title, page_url, score, time, found_hate_texts FROM Page " + where_sql + \
              "ORDER BY score DESC LIMIT %s"
    cursor = mydb.cursor()
    cursor.execute(sql_sel, 20)
    pages = cursor.fetchall()
    return_list = [ShowPage(cursor, page, text_limit) for page in pages]
    cursor.close()
    mydb.close()
    return return_list


# add a click to the appropriate store.
@app.callback(Output('stats_filters', 'data'),
              Input('input_start_date_stats', 'value'),
              Input('input_end_date_stats', 'value'),
              State('stats_filters', 'data'))
def StoreStatFilters(input_start_date, input_end_date, filters):
    filters = {}
    if input_start_date:
        filters['start_date'] = input_start_date
    if input_end_date:
        filters['end_date'] = input_end_date
    return filters

    
@app.callback(Output('stats_div', 'children'),
              Input('stats_filters', 'modified_timestamp'),
              State('stats_filters', 'data'))
def RefreshStats(ts, stats_filters):
    mydb = pymysql.connect(host=settings['MYSQL_HOST'],
                           user=settings['MYSQL_USER'],
                           password=settings['MYSQL_PASSWORD'],
                           database=settings['MYSQL_DB'],
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
    cursor = mydb.cursor()
    if stats_filters:
        if 'start_date' in stats_filters:
            time_start = datetime.strptime(stats_filters['start_date'], '%Y-%m-%d %H:%M:%S')
        else:
            sql_sel = "SELECT time FROM Page ORDER BY time ASC LIMIT 1"
            cursor.execute(sql_sel)
            res = cursor.fetchone()
            time_start = res['time']
        if 'end_date' in stats_filters:
            time_end = datetime.strptime(stats_filters['end_date'], '%Y-%m-%d %H:%M:%S')
        else:
            sql_sel = "SELECT time FROM Page ORDER BY time DESC LIMIT 1"
            cursor.execute(sql_sel)
            res = cursor.fetchone()
            time_end = res['time']
    else:
        sql_sel = "SELECT time FROM Page ORDER BY time ASC LIMIT 1"
        cursor.execute(sql_sel)
        res = cursor.fetchone()
        time_start = res['time']
        sql_sel = "SELECT time FROM Page ORDER BY time DESC LIMIT 1"
        cursor.execute(sql_sel)
        res = cursor.fetchone()
        time_end = res['time']
    
    timedelta = (time_end - time_start)/110
    cursor = mydb.cursor()
    total_domains = []
    hate_domains = []
    total_pages = []
    hate_pages = []
    perc_hate = []
    perc_hate_dom = []
    total_avg_score = []
    hate_avg_score = []
    time_incs = []
    for i in range(0,111):
        stat = []
        datetime_end_inc = time_start + i*timedelta
        time_end_inc = datetime_end_inc.strftime('%Y-%m-%d %H:%M:%S')
        time_incs.append(time_end_inc)
        sql_sel = "SELECT COUNT(DISTINCT(domain)) AS total_domains FROM Page WHERE time < %s;"
        cursor.execute(sql_sel, time_end_inc)
        res = cursor.fetchone()
        total_domains_res = res['total_domains']
        total_domains.append(total_domains_res)
        sql_sel = "SELECT COUNT(*) AS total_pages FROM Page WHERE time < %s;"
        cursor.execute(sql_sel, time_end_inc)
        res = cursor.fetchone()
        total_pages_res = res['total_pages']
        total_pages.append(total_pages_res)
        sql_sel = "SELECT COUNT(*) AS hate_pages FROM Page WHERE score>0.6 AND time < %s;"
        cursor.execute(sql_sel, time_end_inc)
        res = cursor.fetchone()
        hate_pages_res = res['hate_pages']
        hate_pages.append(hate_pages_res)
        if total_pages_res == 0:
            perc_hate_res=0
        else:
            perc_hate_res = hate_pages_res/total_pages_res
        perc_hate.append(perc_hate_res)
        sql_sel = "SELECT COUNT(DISTINCT(domain)) AS hate_domains FROM Page WHERE score>0.6 AND time < %s;"
        cursor.execute(sql_sel, time_end_inc)
        res = cursor.fetchone()
        hate_domains_res = res['hate_domains']
        hate_domains.append(hate_domains_res)
        if total_domains_res == 0:
            perc_hate_dom_res=0
        else:
            perc_hate_dom_res = hate_domains_res/total_domains_res
        perc_hate_dom.append(perc_hate_dom_res)
        sql_sel = "SELECT AVG(score) AS avg_score FROM Page WHERE time < %s;"
        cursor.execute(sql_sel, time_end_inc)
        res = cursor.fetchone()
        total_avg_score.append(res['avg_score'])
        sql_sel = "SELECT AVG(score) AS avg_score FROM Page WHERE score>0.6 AND time < %s;"
        cursor.execute(sql_sel, time_end_inc)
        res = cursor.fetchone()
        hate_avg_score.append(res['avg_score'])
    cursor.close()
    mydb.close()
    children = []
    domain_figure = go.Figure(layout={"title": {"text": "Dominios visitados"}})
    domain_figure.add_trace(go.Scatter(
        x=time_incs,
        y=total_domains,
        mode='lines',
        name='Total'
    ))
    domain_figure.add_trace(go.Scatter(
        x=time_incs,
        y=hate_domains,
        mode='lines',
        name='Relevantes'
    ))
    page_figure = go.Figure(layout={"title": {"text": "Páginas visitadas"}})
    page_figure.add_trace(go.Scatter(
        x=time_incs,
        y=total_pages,
        mode='lines',
        name='Total'
    ))
    page_figure.add_trace(go.Scatter(
        x=time_incs,
        y=hate_pages,
        mode='lines',
        name='Relevantes'
    ))
    perc_hate_figure = go.Figure(layout={"title": {"text": "Porcentaje de elementos relevantes"}})
    perc_hate_figure.add_trace(go.Scatter(
        x=time_incs,
        y=perc_hate,
        mode='lines',
        name='Páginas'
    ))
    perc_hate_figure.add_trace(go.Scatter(
        x=time_incs,
        y=perc_hate_dom,
        mode='lines',
        name='Dominios'
    ))
    avg_score_figure = go.Figure(layout={"title": {"text": "Score medio de las páginas visitadas"}})
    avg_score_figure.add_trace(go.Scatter(
        x=time_incs,
        y=total_avg_score,
        mode='lines',
        name='Total'
    ))
    avg_score_figure.add_trace(go.Scatter(
        x=time_incs,
        y=hate_avg_score,
        mode='lines',
        name='Relevantes'
    ))
    return [dcc.Graph(figure=domain_figure, className='graph'),
            dcc.Graph(figure=page_figure, className='graph'),
            dcc.Graph(figure=perc_hate_figure, className='graph'),
            dcc.Graph(figure=avg_score_figure, className='graph')]

# add a click to the appropriate store.
@app.callback(Output('results_filters', 'data'),
              Input('input_end_date_results', 'value'),
              State('results_filters', 'data'))
def StoreResultsFilters(input_end_date, filters):
    filters = {}
    if input_end_date:
        filters['end_date'] = input_end_date
    return filters


@app.callback(Output('results_div', 'children'),
              Input('results_filters', 'modified_timestamp'),
              State('results_filters', 'data'))
def RefreshResults(ts, results_filters):
    mydb = pymysql.connect(host=settings['MYSQL_HOST'],
                           user=settings['MYSQL_USER'],
                           password=settings['MYSQL_PASSWORD'],
                           database=settings['MYSQL_DB'],
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
    cursor = mydb.cursor()
    if results_filters:
        sql_sel = "SELECT COUNT(*) AS count FROM Page WHERE score >= %s AND score < %s AND time < %s;"
        time_end = datetime.strptime(results_filters['end_date'], '%Y-%m-%d %H:%M:%S')
        incr = []
        counts = []
        for i in range(0,10):
            cursor.execute(sql_sel, (i/10, (i+1)/10, time_end))
            res = cursor.fetchone()
            incr.append("[" + str(i/10) + ", " + str((i+1)/10) + "]")
            counts.append(res['count'])
    else:
        sql_sel = "SELECT COUNT(*) as total_pages, COUNT(DISTINCT(domain)) AS total_domains FROM Page"
        cursor.execute(sql_sel)
        res = cursor.fetchone()
        total_pages = res['total_pages']
        total_domains = res['total_domains']
        sql_sel = "SELECT COUNT(*) as total_pages, COUNT(DISTINCT(domain)) AS total_domains FROM Page WHERE deep_web=true"
        cursor.execute(sql_sel)
        res = cursor.fetchone()
        total_pages_dark = res['total_pages']
        total_domains_dark = res['total_domains']
        sql_sel = "SELECT COUNT(*) as hate_pages, COUNT(DISTINCT(domain)) AS hate_domains FROM Page WHERE score > 0.6;"
        cursor.execute(sql_sel)
        res = cursor.fetchone()
        hate_pages = res['hate_pages']
        hate_domains = res['hate_domains']
        sql_sel = "SELECT COUNT(*) as hate_pages, COUNT(DISTINCT(domain)) AS hate_domains FROM Page WHERE score > 0.6 AND deep_web=true;"
        cursor.execute(sql_sel)
        res = cursor.fetchone()
        hate_pages_dark = res['hate_pages']
        hate_domains_dark = res['hate_domains']
        sql_sel = "SELECT AVG(score) as score FROM Page;"
        cursor.execute(sql_sel)
        res = cursor.fetchone()
        avg_score = res['score']
        sql_sel = "SELECT AVG(score) as score FROM Page WHERE score > 0.6;"
        cursor.execute(sql_sel)
        res = cursor.fetchone()
        avg_score_hate = res['score']
        sql_sel = "SELECT MAX(found_pages) as max_pages, MAX(found_hate_pages) as max_rel_pages, AVG(found_pages) as total_pages, AVG(found_hate_pages) as hate_pages FROM Domain;"
        cursor.execute(sql_sel)
        res_domains = cursor.fetchone()
        avg_total_pages = res_domains['total_pages']
        avg_hate_pages = res_domains['hate_pages']
        sql_sel = "SELECT MAX(found_pages) as max_pages, MAX(found_hate_pages) as max_rel_pages, AVG(found_pages) as total_pages, AVG(found_hate_pages) as hate_pages FROM Domain WHERE found_hate_pages>0;"
        cursor.execute(sql_sel)
        res_rel_domains = cursor.fetchone()
        avg_total_pages_rel = res_rel_domains['total_pages']
        avg_hate_pages_rel = res_rel_domains['hate_pages']
        sql_sel = "SELECT COUNT(*) AS count FROM Domain WHERE found_pages >= %s AND found_pages < %s AND found_hate_pages=0;"
        sql_sel_2 = "SELECT COUNT(*) AS count FROM Domain WHERE found_pages >= %s AND found_pages < %s AND found_hate_pages>0;"
        domain_total_incr = []
        domain_total_list = []
        domain_total_list_rel = []
        incr = math.ceil(res_domains['max_pages']/10)
        for i in range(0,res_domains['max_pages'] + incr,incr):
            cursor.execute(sql_sel, (i, i+incr))
            res = cursor.fetchone()
            domain_total_incr.append("[" + str(i) + ", " + str(i+incr) + "]")
            domain_total_list.append(res['count'])
            cursor.execute(sql_sel_2, (i, i+incr))
            res = cursor.fetchone()
            domain_total_list_rel.append(res['count'])
        domain_hate_list = []
        sql_sel = "SELECT COUNT(*) AS count FROM Page WHERE score >= %s AND score < %s;"
        sql_sel_2 = "SELECT COUNT(*) AS count FROM Page P JOIN Link L ON P.page_url = L.link_url WHERE L.score  >= %s AND L.score < %s;"
        sql_sel_3 = "SELECT COUNT(*) AS count FROM Page P JOIN Link L ON P.page_url = CONCAT(L.link_url, '/') WHERE L.score  >= %s AND L.score < %s;"
        counts_incr = []
        counts = []
        counts_link = []
        for i in range(0,10):
            cursor.execute(sql_sel, (i/10, (i+1)/10))
            res = cursor.fetchone()
            counts_incr.append("[" + str(i/10) + ", " + str((i+1)/10) + "]")
            counts.append(res['count'])
            cursor.execute(sql_sel_2, (i/10, (i+1)/10))
            res = cursor.fetchone()
            cursor.execute(sql_sel_3, (i/10, (i+1)/10))
            res3 = cursor.fetchone()
            counts_link.append(res['count'] + res3['count'])
        sql_sel = "SELECT AVG(L.score - P.score) as avg_dif, STD(L.score - P.score) AS std_dif FROM Page P JOIN Link L ON (P.page_url = L.link_url OR P.page_url = CONCAT(L.link_url, '/'));"
        cursor.execute(sql_sel)
        res = cursor.fetchone()
        avg_dif = res['avg_dif']
        std_dif = res['std_dif']
        sql_sel = "SELECT COUNT(P.page_url) AS count FROM Page P JOIN Link L ON P.page_url = L.link_url WHERE L.score - P.score >= %s AND L.score - P.score  < %s;"
        sql_sel_2 = "SELECT COUNT(P.page_url) AS count FROM Page P JOIN Link L ON P.page_url = CONCAT(L.link_url, '/') WHERE L.score - P.score >= %s AND L.score - P.score  < %s;"
        counts_incr_dif = []
        counts_dif = []
        for i in range(-10,10):
            cursor.execute(sql_sel, (i/10, (i+1)/10))
            res = cursor.fetchone()
            cursor.execute(sql_sel_2, (i/10, (i+1)/10))
            res2 = cursor.fetchone()
            counts_incr_dif.append("[" + str(i/10) + ", " + str((i+1)/10) + "]")
            counts_dif.append(res['count']+res2['count'])

    cursor.close()
    mydb.close()
    score_figure = go.Figure(layout={"title": {"text": "Distribución de páginas visitadas por score"}})
    score_figure.add_bar(
        x=counts_incr,
        y=counts,
        name="Score evaluado"
    )
    score_figure.add_bar(
        x=counts_incr,
        y=counts_link,
        name="Score supuesto antes de visitar"
    )
    domain_figure = go.Figure(layout={"title": {"text": "Distribución de páginas visitadas por dominio"}, "barmode": "stack"})
    domain_figure.add_bar(
        x=domain_total_incr,
        y=domain_total_list
    )
    domain_figure.add_bar(
        x=domain_total_incr,
        y=domain_total_list_rel
    )
    score_figure_2 = go.Figure(layout={"title": {"text": "Distribución de diferencias"}})
    score_figure_2.add_bar(
        x=counts_incr_dif,
        y=counts_dif
    )
    return [html.Span(children="Dominios totales: " + str(total_domains)),
            html.Span(children="Páginas totales: " + str(total_pages)),
            html.Span(children="Dominios relevantes: " + str(hate_domains)),
            html.Span(children="Páginas relevantes: " + str(hate_pages)),
            html.Span(children="Dominios de dark web totales: " + str(total_domains_dark)),
            html.Span(children="Páginas de dark web totales: " + str(total_pages_dark)),
            html.Span(children="Dominios de dark web relevantes: " + str(hate_domains_dark)),
            html.Span(children="Páginas de dark web relevantes: " + str(hate_pages_dark)),
            html.Span(children="Media de score páginas: " + str(avg_score)),
            html.Span(children="Media de score páginas relevantes: " + str(avg_score_hate)),
            html.Span(children="Media páginas relevantes: " + str(avg_hate_pages)),
            html.Span(children="Media páginas totales: " + str(avg_total_pages)),
            html.Span(children="Media páginas relevantes en dominios relevantes: " + str(avg_hate_pages_rel)),
            html.Span(children="Media páginas totales en dominios relevantes: " + str(avg_total_pages_rel)),
            html.Span(children="Media diferencia: " + str(avg_dif)),
            html.Span(children="STD diferencia: " + str(std_dif)),
            dcc.Graph(figure=score_figure, className='graph'),
            dcc.Graph(figure=score_figure_2, className='graph'),
            dcc.Graph(figure=domain_figure, className='graph')]


if __name__ == '__main__':
    app.run_server(debug=True) 
