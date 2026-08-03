#!/bin/sh
# ============================================================================
# java-tsfile-api-test 统一入口
# 测试框架: TestNG + Maven surefire  |  CASE_ID: ClassName.methodName
# ============================================================================
set -u

CMD="${1:-help}"
TGT="${2:-}"
REPORTS_DIR="${TSFILE_REPORT_DIR:-reports}"

# ---- proxy (不硬编码地址，从环境变量 TEST_PROGRAM_PROXY 读取) ----
proxy_apply() {
  if [ -n "${TEST_PROGRAM_PROXY:-}" ]; then
    export HTTPS_PROXY="$TEST_PROGRAM_PROXY"
    export HTTP_PROXY="$TEST_PROGRAM_PROXY"
    echo "[proxy] $TEST_PROGRAM_PROXY"
  else
    echo "[proxy] WARNING: TEST_PROGRAM_PROXY 未设置，--proxy 无效" >&2
  fi
}
for a in "$@"; do [ "$a" = "--proxy" ] && proxy_apply; done

usage() { cat >&2 <<'EOF'
Usage: sh start.sh <command> [target] [--proxy]
  prepare            编译 TsFile JAR + 安装到本地 Maven 仓库 + 下载依赖
  all                mvn test (全部)
  page <name>        page table | page tree
  case <case-id>     ClassName.methodName  (例: TestITsFileReader.testQuery1)
  cases <id1,id2>    批量 (逗号 → surefire + 连接)
  plm / plm-all      PLM 全量
  list-cases --json  用例清单 (JSON)
  help               帮助

Proxy: 设置环境变量后使用 --proxy
  export TEST_PROGRAM_PROXY=http://your-proxy:port
  sh start.sh <command> --proxy
EOF
}

# ---- prepare ----
prepare() {
  local ts="../tsfile"
  [ -f "$ts/pom.xml" ] || { echo "ERROR: $ts 不存在" >&2; return 1; }
  echo "=== 安装 TsFile JAR 到本地 Maven 仓库 ==="
  (cd "$ts" && mvn install -P with-java -DskipTests -Dspotless.check.skip=true -q) || { echo "ERROR: TsFile 编译失败" >&2; return 1; }
  echo "=== 编译测试项目 + 下载依赖 ==="
  mvn compile test-compile 2>&1 | tail -3
  echo "=== prepare 完成 ==="
}

list_cases() { python3 - "$(pwd)" << 'PYEOF'
import json,os,re,sys
src=os.path.join(sys.argv[1],'src/test/java'); cs=[]
for r,_,fs in os.walk(src):
 for f in sorted(fs):
  if not f.endswith('.java'): continue
  fp=os.path.join(r,f)
  cls=os.path.relpath(fp,src).replace('/','.').replace('\\','.').replace('.java','')
  with open(fp,'r',encoding='utf-8',errors='replace') as fh: txt=fh.read()
  for m in re.finditer(r'public\s+void\s+(test\w+)\s*\(',txt):
   cs.append({'caseId':f'{cls}.{m.group(1)}','automationType':'ts-java','nodeId':f'{cls}::{m.group(1)}','sourceFile':os.path.relpath(fp,sys.argv[1]),'description':''})
print(json.dumps(cs,ensure_ascii=False,indent=2))
PYEOF
}

all()  { mkdir -p "$REPORTS_DIR"; mvn test 2>&1; echo "=== target/surefire-reports ==="; }
page() {
  case "$1" in table) PKG="org.apache.tsfile.table";; tree) PKG="org.apache.tsfile.tree";;
    *) echo "Unknown: $1" >&2; return 2;; esac
  CLASSES=$(find src/test/java -path "*${PKG//./\/*}" -name '*Test*.java' 2>/dev/null | sed 's|src/test/java/||;s|/|.|g;s|\.java$||' | paste -sd '+' -)
  [ -z "$CLASSES" ] && { echo "No test classes for: $1" >&2; return 1; }
  mkdir -p "$REPORTS_DIR"; mvn test -Dtest="$CLASSES" -DfailIfNoTests=false 2>&1
}
case_one()    { [ -z "$1" ] && { usage; return 2; }; mkdir -p "$REPORTS_DIR"; mvn test -Dtest="$1" -DfailIfNoTests=false 2>&1; }
cases_batch() { [ -z "$1" ] && { usage; return 2; }; mkdir -p "$REPORTS_DIR"; mvn test -Dtest="$(echo "$1" | sed 's/,/+/g')" -DfailIfNoTests=false 2>&1; }
plm() { echo "[plm] 全量测试"; all; }

case "$CMD" in
  prepare) prepare;;  all) all;;  page) page "$TGT";;
  case|case-id) case_one "$TGT";;  cases) cases_batch "$TGT";;
  plm|plm-all) plm;;  list-cases) list_cases;;  help|-h|--help) usage;;
  *.*) case_one "$CMD";;  *) echo "Unknown: $CMD" >&2; usage; exit 2;;
esac
