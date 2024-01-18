import os, shutil

def save_best_model(**kwargs):
    ti = kwargs['ti']
    xcom_keys = ti.xcom_pull(
        key='predict_acc_result',
        task_ids=[
            'predict_model_svm',
            'predict_model_RandomForest',
            'predict_model_LogisticRegression',
            'predict_model_GradientBoosted',
            'predict_model_DecisionTrees',
            'predict_model_SVM'
        ],
        include_prior_dates=True)

    if xcom_keys:
        max_acc = 0
        max_acc_model_path = None

        for xcom_key in xcom_keys:
            percentage_matching = xcom_key['percentage_matching']
            model_path = xcom_key['model_path']

            if percentage_matching > max_acc:
                max_acc = percentage_matching
                max_acc_model_path = model_path

        best_model_name = max_acc_model_path.split('/')[-1]
        best_model_path = os.path.expanduser(f'~/code/ie212.o11.group11/models/best_model_{best_model_name}_{max_acc}')
        
        if os.path.exists(best_model_path):
            shutil.rmtree(best_model_path)
        shutil.copytree(max_acc_model_path, best_model_path)
    else:
        print('No xcom_keys exists')