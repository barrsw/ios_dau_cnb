# Databricks notebook source
# MAGIC %run Repos/apps/ios-dau/utils/infra_utils

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
import pytz

def divide_chunks(l, n):
    
    for i in range(0, len(l), n):
        yield l[i:i + n]

def blocking_call_of_notebooks_parallelization(notebooks, timeout):
    return [(notebook[0].result(timeout=timeout), notebook[1]) for notebook in notebooks]
        
def parallelNotebooks(notebooks, numInParallel):
   # If you create too many notebooks in parallel the driver may crash when you submit all of the jobs at once. 
   # This code limits the number of parallel notebooks.
    with ThreadPoolExecutor(max_workers=numInParallel) as ec:
        return [(ec.submit(Notebook.submitNotebook, notebook), notebook.parameters["date"]) for notebook in notebooks]
    
def parallelNotebooksByMode(notebooks, numInParallel, parallelization_mode, timeout):
    
    if parallelization_mode == 'parallel':
        results =  parallelNotebooks(notebooks, numInParallel)
        return blocking_call_of_notebooks_parallelization(results, timeout)
    
    if parallelization_mode == 'bulk':
        chuncked_notebooks = divide_chunks(notebooks, numInParallel) # divide all the notebooks to chuncks
        results = []
        for notebooks_chunck in chuncked_notebooks:
            bulk = parallelNotebooks(notebooks_chunck, numInParallel)
            results.extend(blocking_call_of_notebooks_parallelization(bulk, timeout)) # This is a blocking call.
        return results
            
    else:
        raise Exception("parallelization_mode has wrong value!")
    
#result = [(i[0].result(timeout=timeout), i[1]) for i in res] # This is a blocking call.
class Notebook:
    def __init__(self, path, timeout, parameters=None, retry=0):
        self.path = path
        self.timeout = timeout
        self.parameters = parameters
        self.retry = retry
        
    def submitNotebook(notebook):
        print("[%s] Running notebook '%s' of date %s" % (datetime.now(pytz.timezone('Asia/Jerusalem')), notebook.path, notebook.parameters["date"]))
        try:
            if (notebook.parameters):
                dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
                return "Success"
            else:
                dbutils.notebook.run(notebook.path, notebook.timeout)
                return "Success"
        except Exception as e:
            if notebook.retry < 1:
                print("[%s] Notebook '%s' of date %s has failed with the following exception: %s" %
                      (datetime.now(pytz.timezone('Asia/Jerusalem')), notebook.path, notebook.parameters["date"], str(e)))
                return "Failed"
            else:
                print("[%s] Retrying notebook '%s' of date %s" % (datetime.now(pytz.timezone('Asia/Jerusalem')), notebook.path, notebook.parameters["date"]))
                notebook.retry = notebook.retry - 1
                return submitNotebook(notebook)

    def calc_daily_executions(start_date, end_date):
        return [start_date + timedelta(days=i) for i in range((end_date-start_date).days + 1)]
    
    def calc_weekly_executions(start_date, end_date):
        if start_date.weekday() != 6: # if start_date isn't sunday get start_date as next sunday
            idx = (start_date.weekday() + 1) % 7
            start_date = start_date + timedelta(7-idx) # Next sunday
            print("start_date is modifed to " + str(start_date))
        return [start_date + timedelta(7*d) for d in range(int((end_date-start_date).days/7)+1)]

    def calc_monthly_executions(start_date, end_date):
        if start_date.day != 1: # if start_date isn't first day of month
            raise Exception("Dates in monthly mode should start on the first day of the month")
        dates = []
        while start_date <= end_date:
            dates.append(start_date)
            start_date = start_date + relativedelta.relativedelta(months=1, day=1)
        return dates

    def get_dates_range(start_date, end_date, mode='daily'):
        assert mode in ["daily", "weekly", "monthly"]
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        execution_funcs = {"daily": calc_daily_executions, "weekly": calc_weekly_executions, "monthly": calc_monthly_executions}
        return execution_funcs[mode](start_date, end_date)
    
    def process_dates_list(dates_list):
        return [datetime.strptime(d, "%Y-%m-%d").date() if isinstance(d, str) else d for d in dates_list]     

    def runner(notebook_name, arguments, start_date=None, end_date=None, mode='daily', parallelization_mode='parallel', dates_list=None, timeout=6000, parallel_runs=10, retry=0):
        assert (bool(start_date and end_date) != bool(dates_list)), "Pass either start_date and end_date or dates_list"
        assert parallelization_mode in ['parallel', 'bulk'], "parallelization_mode should be either 'parallel' or 'bulk'"
        if start_date is not None and end_date is not None:
            dates = get_dates_range(start_date, end_date, mode)
        else:
            dates = Notebook.process_dates_list(dates_list)
        notebooks = []
        for d in dates:
            arg = arguments.copy()
            arg["date"] = d.strftime("%Y-%m-%d")
            notebooks.append(Notebook(notebook_name, timeout, arg, retry)) 

        results = parallelNotebooksByMode(notebooks, parallel_runs, parallelization_mode, timeout)
        failed_notebooks = [i[1] for i in results if i[0] == "Failed"]
        successed_notebooks = [i[1] for i in results if i[0] == "Success"]
        if len(failed_notebooks) == 0:
            print('All runs finished successfully!')
        else:
            print('There are failed runs on those dates: ' + ", ".join(failed_notebooks) + "\nSuccessful runs: " + ", ".join(successed_notebooks))
            
    def verify_success_marker(path, start_date, end_date, mode='daily'):
        dates = get_dates_range(start_date, end_date, mode)
        success = []
        no_success = []
        for d in dates:
            try:
                dbutils.fs.ls(path + "/" + add_year_month_day(d) + "/_SUCCESS")
                success.append(str(d))
            except:
                no_success.append(str(d))
        print("The following dates have success marker: " + ", ".join(success))
        print("The following dates don't have success marker: " + ", ".join(no_success))