# Ghi đè môi trường an toàn
source .env
# Commit, push, và snapshot môi trường (ctx)
make ctx | tee build/reports/ctx_$(date +%Y%m%d_%H%M%S).json
git add .
git commit -m "sync: $(date +'%Y-%m-%d %H:%M') auto snapshot"
git push

