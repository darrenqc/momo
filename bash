# find expired sessionId
grep -oP 'sessionId .*> Status not OK' momo.recommend.log2017-11-1*|awk -F' ' '{print $2}'|awk -F'>' '{print $1}'|sort|uniq -c > ~/forbid