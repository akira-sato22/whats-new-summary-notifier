"""Lambda関数のindex.pyを名前衝突なくテストから読み込むためのヘルパー"""

import importlib.util
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_lambda_module(module_name, lambda_dir, env=None):
    """lambda/<lambda_dir>/index.py を独立したモジュールとして読み込む

    各Lambdaのエントリポイントは同名(index.py)のため、
    importlib で一意なモジュール名を付けて読み込む。
    モジュールはimport時にboto3クライアントを生成するので、
    必要な環境変数を先に設定する。

    Args:
        module_name (str): 読み込み後のモジュール名
        lambda_dir (str): lambda/ 配下のディレクトリ名
        env (dict): import前に設定する環境変数

    Returns:
        module: 読み込んだモジュール
    """
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    for key, value in (env or {}).items():
        os.environ.setdefault(key, value)

    path = os.path.join(REPO_ROOT, "lambda", lambda_dir, "index.py")
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
