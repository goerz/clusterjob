rsync -av {remote}:{workdir} ./runfolder
ssh {remote} 'rm -f {workdir}/*'
ssh {remote} 'rm -f {workdir}/.*'
ssh {remote} 'rmdir {workdir}'
