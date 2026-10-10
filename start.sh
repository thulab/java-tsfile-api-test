#!/bin/sh
# ============================================================================
# java-tsfile-api-test 统一入口  v2.1
# 测试框架: TestNG + Maven surefire  |  CASE_ID: ClassName.methodName
# ============================================================================
set -u

# —— tsfile 依赖版本解析（R1 修复，2026-10-10）——
# 从产品源码 ../tsfile/pom.xml 读取当前 version，注入所有测试侧 mvn 调用，
# 防止解析到 m2 中过期的 tsfile jar（2026-10-10 审查发现：曾固定 2.3.2-SNAPSHOT）。
# 可用 TSFILE_VERSION 环境变量显式覆盖；解析块只依赖 python3，list-cases 不受影响。
_PRODUCT_POM="../tsfile/pom.xml"
TSFILE_VER=""
if [ -f "$_PRODUCT_POM" ]; then
  TSFILE_VER=$(python3 -c "import xml.etree.ElementTree as ET;root=ET.parse('$_PRODUCT_POM').getroot();vals=[e.text for e in list(root) if e.tag.split('}')[-1]=='version'];print(vals[0] if vals else '')" 2>/dev/null || true)
fi
if [ -n "${TSFILE_VERSION:-}" ]; then TSFILE_VER="$TSFILE_VERSION"; fi
if [ -z "$TSFILE_VER" ] || [ "$TSFILE_VER" = "None" ]; then
  echo "[start.sh] WARNING: 无法从 $_PRODUCT_POM 解析产品版本；mvn 类命令将失败（可用 TSFILE_VERSION=… 覆盖）" >&2
  TSFILE_VER="UNSET"
fi

CMD="${1:-help}"
TGT="${2:-}"
REPORTS_DIR="${REPORT_DIR:-${TSFILE_REPORT_DIR:-reports}}"

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
  case <case-id>     ClassName.methodName
  cases <id1,id2>    批量 (逗号分隔)
  plm / plm-all      PLM 全量
  list-cases --json  用例清单 (JSON)
  help               帮助
EOF
}

prepare() {
  local ts="../tsfile"
  [ -f "$ts/pom.xml" ] || { echo "ERROR: $ts 不存在" >&2; return 1; }
  echo "=== 安装 TsFile JAR 到本地 Maven 仓库 ==="
  (cd "$ts" && mvn install -P with-java -DskipTests -Dspotless.check.skip=true -q) || { echo "ERROR: TsFile 编译失败" >&2; return 1; }
  echo "=== 编译测试项目 + 下载依赖（tsfile.version=$TSFILE_VER）==="
  mvn -Dtsfile.version="$TSFILE_VER" compile test-compile 2>&1 | tail -3
  echo "=== prepare 完成 ==="
}

list_cases() { python3 - "$(pwd)" << 'PYEOF'
import json,os,re,sys
src=os.path.join(sys.argv[1],'src/test/java')
cases=[]; id_map={}
for r,_,fs in os.walk(src):
 for f in sorted(fs):
  if not f.endswith('.java'): continue
  fp=os.path.join(r,f)
  cls=os.path.relpath(fp,src).replace('/','.').replace('\\','.').replace('.java','')
  with open(fp,'r',encoding='utf-8',errors='replace') as fh: txt=fh.read()
  txt=re.sub(r'//[^\n]*','',txt)
  txt=re.sub(r'/\*.*?\*/','',txt,flags=re.DOTALL)
  for m in re.finditer(r'public\s+void\s+(test\w+)\s*\(',txt):
   mn=m.group(1); cid=f'{cls}.{mn}'
   cases.append({'caseId':cid,'automationType':'ts-java','nodeId':f'{cls}::{mn}','sourceFile':os.path.relpath(fp,sys.argv[1]),'description':''})
   id_map[cid.upper()]=f'{cls}#{mn}'
_map_path=os.path.join(sys.argv[1],'.plm-case-id-map.json')
_skip=False  # heuristic: PLM CASE_ID (e.g. Java-Table-001) contains '-', generated IDs don't

if os.path.exists(_map_path):
 try:
  with open(_map_path) as _f: _existing=json.load(_f)
  _keys=list(_existing.keys()) if isinstance(_existing,dict) else []
  if _keys and ('nmap' in _existing or any('-' in str(k) for k in _keys)): _skip=True; print('[start.sh] PLM CASE_ID map 已存在，跳过覆盖',file=sys.stderr)
 except: pass
if not _skip:
 with open(_map_path,'w') as fh: json.dump(id_map,fh)
cn_map = {}
try:
    with open(os.path.join(sys.argv[1],'.plm-cn-names.json')) as f: cn_map = json.load(f)
except: pass
# Remap to PLM CASE_IDs via .plm-case-id-map.json（nmap: PLM CASE_ID → nodeId 反查）
_pm = {}
try:
 with open(os.path.join(sys.argv[1],'.plm-case-id-map.json')) as _f2: _mm = json.load(_f2)
 for _pid, _v in (_mm.get('nmap') or {}).items():
  for _x in (_v if isinstance(_v, list) else [str(_v)]): _pm[_x] = _pid
except: pass
for _c in cases:
 _cid=_pm.get(_c['nodeId'])
 if _cid:
  _c['caseId']=_cid
  _c['name'] = cn_map.get(_cid) or _cid
print(json.dumps(cases,ensure_ascii=False,indent=2))
PYEOF
}

all()  { mkdir -p "$REPORTS_DIR"; mvn -Dtsfile.version="$TSFILE_VER" test 2>&1; echo "=== target/surefire-reports ==="; }
page() {
  case "$1" in table) PKG="org.apache.tsfile.table";; tree) PKG="org.apache.tsfile.tree";;
    *) echo "Unknown: $1" >&2; return 2;; esac
  CLASSES=$(find src/test/java -path "*${PKG//./\/*}" -name '*Test*.java' 2>/dev/null | sed 's|src/test/java/||;s|/|.|g;s|\.java$||' | paste -sd '+' -)
  [ -z "$CLASSES" ] && { echo "No test classes for: $1" >&2; return 1; }
  mkdir -p "$REPORTS_DIR"; mvn -Dtsfile.version="$TSFILE_VER" test -Dtest="$CLASSES" -DfailIfNoTests=false 2>&1
}

case_one() {
  [ -z "$1" ] && { usage; return 2; }
  mkdir -p "$REPORTS_DIR"
  local mf=".plm-case-id-map.json"
  if [ ! -f "$mf" ]; then echo "[start.sh] .plm-case-id-map.json missing, restore backup" >&2; return 5; fi
  local sfp; sfp=$(python3 map_lookup.py "$1" 2>/dev/null)
  rm -rf target/surefire-reports
  timeout 240 mvn -Dtsfile.version="$TSFILE_VER" test -Dtest="$sfp" -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false 2>&1
  local mvn_rc=$?
  local post_rc=0
  python3 -c "
import os,sys
from xml.etree import ElementTree as ET
ET.register_namespace('','')
sr='target/surefire-reports';target='$REPORTS_DIR/case-$1.xml'
if not os.path.isdir(sr): sys.exit(2)
root=ET.Element('testsuites');cnt=0
for f in sorted(os.listdir(sr)):
 if not f.startswith('TEST-') or not f.endswith('.xml'): continue
 tree=ET.parse(os.path.join(sr,f));suite=tree.getroot()
 for tc in suite.iter('testcase'):
  cn=tc.get('classname','');nm=tc.get('name','')
  cid=f'{cn}.{nm}' if cn and nm else nm
  if not cid: continue
  props=tc.find('properties')
  if props is None: props=ET.SubElement(tc,'properties')
  ok=[p.get('value') for p in props.findall('property') if p.get('name')=='case_id']
  if cid not in ok: ET.SubElement(props,'property',{'name':'case_id','value':cid})
 root.append(suite);cnt+=1
if cnt==0: print('[start.sh] WARNING: no surefire XML',file=sys.stderr);sys.exit(3)
ET.ElementTree(root).write(target,encoding='utf-8',xml_declaration=True)
" 2>/dev/null; post_rc=$?
  if [ $post_rc -ne 0 ]; then
    echo "[start.sh] XML post-process failed (exit=$post_rc)" >&2
    rm -f "$REPORTS_DIR/case-$1.xml"
    return 1
  fi
  # Fix: replace internal class name with PLM CASE_ID in JUnit XML
  sed -i "s|name=\"case_id\" value=\"[^\"]*\"|name=\"case_id\" value=\"$1\"|" "$REPORTS_DIR/case-$1.xml"
  return $mvn_rc
}

cases_batch() {
  [ -z "$1" ] && { usage; return 2; }
  mkdir -p "$REPORTS_DIR"
  local mf=".plm-case-id-map.json"
  if [ ! -f "$mf" ]; then echo "[start.sh] .plm-case-id-map.json missing, restore backup" >&2; return 5; fi
  local result; result=$(python3 -c "
import json
with open('$mf') as f: m=json.load(f)
ids='$1'.split(',')
patterns=[]; missing=[]
for i in ids:
    i=i.strip()
    if not i: continue
    v=None; uc=i.upper()
    for k, kv in m.items():
        if k.upper()==uc:
            v=kv; break
    if v:
        if isinstance(v,list):
            for x in v: patterns.append(x.replace('::','#'))
        else:
            patterns.append(str(v).replace('::','#'))
    else:
        missing.append(i)
print('PATTERNS:'+','.join(patterns) if patterns else 'PATTERNS:')
print('MISSING:'+' '.join(missing) if missing else 'MISSING:')
" 2>/dev/null)
  local sfps=$(echo "$result" | grep '^PATTERNS:' | sed 's/^PATTERNS://')
  local missing_ids=$(echo "$result" | grep '^MISSING:' | sed 's/^MISSING://')
  if [ -n "$missing_ids" ]; then
    echo "[start.sh] CASE_ID(s) not found: $missing_ids" >&2
  fi
  if [ -z "$sfps" ]; then
    echo "[start.sh] No matching tests" >&2
    return 5
  fi
  # Build reverse map: surefire_key -> PLM CASE_ID
  local rev_map_json; rev_map_json=$(python3 -c "
import json,os
map_path=os.path.join('$(pwd)','.plm-case-id-map.json')
rev={}
try:
 with open(map_path) as f: m=json.load(f)
 nmap=m.get('nmap',m); al=m.get('aliases') or {}
 def _lk(cid,o):
  return o.get(cid) or next((kv for k,kv in o.items() if k.upper()==cid.upper()), None)
 for cid in [x.strip() for x in '$1'.split(',') if x.strip()]:
  v=_lk(cid,nmap) or _lk(cid,al)
  if not v: continue
  for s in (v if isinstance(v,list) else [str(v)]):
   rev[s.replace('::','#')]=cid
except: pass
print(json.dumps(rev))
" 2>/dev/null)
  rm -rf target/surefire-reports
  timeout 240 mvn -Dtsfile.version="$TSFILE_VER" test -Dtest="$sfps" -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false 2>&1
  local mvn_rc=$?
  local post_rc=0
  python3 -c "
import os,sys,json
from xml.etree import ElementTree as ET
ET.register_namespace('','')
rev_map=json.loads('$rev_map_json' or '{}')
sr='target/surefire-reports';target='$REPORTS_DIR/cases.xml'
if not os.path.isdir(sr): sys.exit(2)
root=ET.Element('testsuites');cnt=0
for f in sorted(os.listdir(sr)):
 if not f.startswith('TEST-') or not f.endswith('.xml'): continue
 tree=ET.parse(os.path.join(sr,f));suite=tree.getroot()
 for tc in suite.iter('testcase'):
  cn=tc.get('classname','');nm=tc.get('name','')
  internal=cn+'.'+nm if cn and nm else nm
  # Use PLM CASE_ID from reverse map, fallback to internal name
  sf_key=cn+'.'+nm if cn and nm else nm
  sf_key=sf_key[:sf_key.rfind('.')]+'#'+sf_key[sf_key.rfind('.')+1:] if '.' in sf_key else sf_key
  plm_cid=rev_map.get(sf_key,rev_map.get(internal,internal))
  props=tc.find('properties')
  if props is None: props=ET.SubElement(tc,'properties')
  ok=[p.get('value') for p in props.findall('property') if p.get('name')=='case_id']
  if plm_cid not in ok: ET.SubElement(props,'property',{'name':'case_id','value':plm_cid})
 root.append(suite);cnt+=1
if cnt==0: print('[start.sh] WARNING: no surefire XML',file=sys.stderr);sys.exit(3)
ET.ElementTree(root).write(target,encoding='utf-8',xml_declaration=True)
" 2>/dev/null; post_rc=$?
  if [ $post_rc -ne 0 ]; then
    echo "[start.sh] XML post-process failed (exit=$post_rc)" >&2
    rm -f "$REPORTS_DIR/cases.xml"
    return 1
  fi
  return $mvn_rc
}

plm() { echo "[plm] 全量测试"; all; }

case "$CMD" in
  prepare) prepare;;  all) all;;  page) page "$TGT";;
  case|case-id) case_one "$TGT" || exit;;  cases) cases_batch "$TGT" || exit;;
  plm|plm-all) plm;;  list-cases) list_cases;;  help|-h|--help) usage;;
  *.*) case_one "$CMD";;  *) echo "Unknown: $CMD" >&2; usage; exit 2;;
esac
