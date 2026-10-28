"""python -I announcement_lab/serve.py; no base project dependency changes."""
import argparse
import os
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
if hasattr(sys.stdout,'reconfigure'):sys.stdout.reconfigure(encoding='utf-8')


def main():
    parser=argparse.ArgumentParser(description='本机公开公告证据研究，行情和分享服务保持独立')
    parser.add_argument('--port',type=int,default=8010)
    parser.add_argument('--data-root')
    args=parser.parse_args()
    if args.data_root:os.environ['ANNOUNCEMENT_DATA_ROOT']=str(Path(args.data_root).resolve())
    from announcement_lab.api import create_app
    import uvicorn
    app=create_app()
    uvicorn.run(app,host=os.getenv('ANNOUNCEMENT_BIND_HOST','127.0.0.1'),port=args.port,
                access_log=False,log_level='warning')


if __name__=='__main__':main()
