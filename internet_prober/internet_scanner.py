import json
import asyncio
import logging

async def run_yarrp(conf):
    yarrp_command = [
        f'{conf["yarrp_dir"]}/yarrp',
        '-o', conf['yarrp']['intermediate_output_file'],
        '-i', conf['yarrp']['target_file'],
        '-r', str(conf['yarrp']['probe_rate']),
        '-t', conf['yarrp']['probe_type'],
        '-v',
        '-m', str(conf['yarrp']['max_ttl']),
    ]

    command = ' '.join(yarrp_command)
    logging.info(f"Executing command: {command}")
    process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()

    if process.returncode == 0:
        logging.info(f'Command succeeded: {stdout.decode()}')
    else:
        logging.info(f'Command failed: {stderr.decode()}')
