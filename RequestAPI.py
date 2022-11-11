import pandas as pd
import requests
import base64

class CentralIODS():

    methods = [
        {'name': 'get_sites', 'parameters': ''},
        {'name': 'get_lines', 'parameters': 'site'},
        {'name': 'get_units', 'parameters': 'site,lines'},
        {'name': 'get_kpi', 'parameters': ''},
        {'name': 'get_date_from_time_option', 'parameters': 'site,lines,time_option'},
        {'name': 'get_raw_data', 'parameters': 'site,item,lines,units,start_time,end_time'},
        {'name': 'get_kpi_value', 'parameters': 'site,equipments,time_option,kpis,filter_type,pr_option,team,shift'},
        {'name': 'get_kpi_monthly', 'parameters': 'site, equipments, start_time,end_time,kpis,filter_type,pr_option,team,shift'},
        {'name': 'get_kpi_production_day', 'parameters': 'site,equipments,start_time,end_time,kpis,filter_type,pr_option,team,shift'}
    ]
    rootAPI = 'https://internalapi.pg.com/supplychain/manufacturingstandard/v1/iods/'

    def __init__(self, username, password):
        credencials = f'{username}:{password}'
        credencials_encoded = base64.b64encode(credencials.encode('utf-8'))
        self.headers = {
            'Authorization': f'Basic '+ credencials_encoded.decode('ascii'),
            'Ocp-Apim-Subscription-Key': 'd65b42c29d56497c979cb51d805ce6ac',
        }

    def to_data_frame(self, response):
        if type(response) is not list:
            if response and 'Message' in response and 'MessageDetail' in response:
                raise Exception('Request data failed: {} {}'.format(response['Message'],
                                                                    response['MessageDetail']))
            else:
                raise Exception('Request data failed: Unknown error.')
        else:
            if len(response) > 0:
                # convert List to Dataframe
                data_frame = pd.DataFrame(response, columns=list(response[0].keys()))
            else:
                return pd.DataFrame()
        return data_frame

    def get_sites(self):
        data_frame = pd.DataFrame()
        url = f'{ self.rootAPI }filters/sites'

        # get the URL as json
        response = requests.get(url, headers=self.headers).json()

        return self.to_data_frame(response)

    def get_lines(self, site):
        data_frame = pd.DataFrame()
        url = f'{ self.rootAPI }filters/lines?site={ site }'

        # get the URL as json
        response = requests.get(url, headers=self.headers).json()

        return self.to_data_frame(response)

    def get_units(self, site, lines):
        data_frame = pd.DataFrame()
        url = f'{ self.rootAPI }filters/units?site={ site }&lines={ lines }'

        # get the URL as json
        response = requests.get(url, headers=self.headers).json()

        return self.to_data_frame(response)

    def get_kpi(self):
        data_frame = pd.DataFrame()
        url = f'{ self.rootAPI }filters/kpis'

        # get the URL as json
        response = requests.get(url, headers=self.headers).json()

        return self.to_data_frame(response)

    def get_date_from_time_option(self, site, lines, time_option):
        data_frame = pd.DataFrame()
        url = f'{ self.rootAPI }filters/date-from-time-option?site={ site }&lines={ lines }&timeOption={ time_option }'

        # get the URL as json
        response = requests.get(url, headers=self.headers).json()

        return self.to_data_frame(response)

    def get_raw_data(self, site, rawdata, lines, units, start_time, end_time):
        page = 1
        data_frame = pd.DataFrame()
        assert rawdata.lower() in ['downtimeuptime', 'reject', 'production', 'alarm', 'edefects', 'npt','quality', 'centerline', 'cil']

        switcher = {
            'quality': "variable/qa",
            'centerline': "variable/cl",
            'cil': "variable/cil",
        }
        if rawdata in switcher:
            rawdata = switcher[rawdata]

        while page != 0:
            # Build a valid API URL
            url = f'{ self.rootAPI }{ rawdata }/rawdata?site={ site }&startTime={ start_time }&endTime={ end_time }&lines={ lines }&units={ units }&pageSize=10000&pageNum=' + str(page)
            # get the URL as json
            response = requests.get(url, headers=self.headers).json()

            temp_df = self.to_data_frame(response)
            if len(temp_df.index) > 0:
                data_frame = data_frame.append(temp_df)
                page += 1
            else:
                page = 0

        return data_frame

    def get_kpi_value(self, site, equipments, time_option, kpis, filter_type, pr_option, team, shift):
        data_frame = pd.DataFrame()
        url = f'{ self.rootAPI }kpi?site={ site }&equipments={ equipments }&timeOption={ time_option }&kpis={ kpis }&filterType={ filter_type }&prOption={ pr_option }&team={ team }&shift={ shift }'

        # get the URL as json
        response = requests.get(url, headers=self.headers).json()

        return self.to_data_frame(response)

    def get_kpi_monthly(self, site, equipments, start_time, end_time, kpis, filter_type, pr_option, team, shift):
        data_frame = pd.DataFrame()
        url = f'{ self.rootAPI }kpi/monthly?site={ site }&equipments={ equipments }&startTime={ start_time }&endTime={ end_time }&kpis={ kpis }&filterType={ filter_type }&prOption={ pr_option }&team={ team }&shift={ shift }'

        # get the URL as json
        response = requests.get(url, headers=self.headers).json()

        return self.to_data_frame(response)

    def get_kpi_production_day(self, site, equipments, start_time, end_time, kpis, filter_type, pr_option, team, shift):
        data_frame = pd.DataFrame()
        url = f'{ self.rootAPI }kpi/production-day?site={ site }&equipments={ equipments }&startTime={ start_time }&endTime={ end_time }&kpis={ kpis }&filterType={ filter_type }&prOption={ pr_option }&team={ team }&shift={ shift }'

        # get the URL as json
        response = requests.get(url, headers=self.headers).json()

        return self.to_data_frame(response)

centra_iods = CentralIODS(username='', password='')
# site_list = centra_iods.get_sites()
# line_list = centra_iods.get_lines('Luogang')
# kpi =centra_iods.get_kpi_production_day(site = 'Xiqing',
#                                          equipments = 'DIAH121',
#                                          start_time = '2022-03-01 00:00',
#                                          end_time = '2022-03-18 00:00',
#                                          kpis = 'TotalStops,TotalProduct,GoodProduct,RunningScrap,StartingScrap,Scheduletime,TotalScrap,PR',
#                                          filter_type = 'lines',
#                                          pr_option = 'PR In:',
#                                          team = 'All',
#                                          shift = 'All')

quality = centra_iods.get_raw_data(
                                   site='Luogang',
                                   # site='Xiqing',
                                   rawdata='quality',
                                   # lines='DIAH121',
                                   lines='DILL183',
                                   units=None,
                                   start_time='2022-10-20 00:00',
                                   end_time='2022-10-20 23:30')

strvarid = [143244,143245,143246,143247]
filter_quality = quality[quality.VarId.isin(strvarid)]
filter_quality.to_csv('./CP83_FMOT.csv', index=False)
