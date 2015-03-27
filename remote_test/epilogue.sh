rsync -av {remote}:{fulldir}/ ./runfolder
ssh {remote} 'rm -f {fulldir}/*'
ssh {remote} 'rm -f {fulldir}/.*'
ssh {remote} 'rmdir {fulldir}'
