import json
import asyncio
import logging

async def run(conf):
    yarrp_command = [
        '/yarrp/yarrp',
        '-o', conf['prober']['tmp_output_file'],
        '-i', conf['prober']['targets_file'],
        '-r', str(conf['prober']['probe_rate']),
        '-t', conf['prober']['probe_type'],
        '-v',
        '-m', str(conf['prober']['max_ttl']),
    ]

    command = ' '.join(yarrp_command)
    logging.info(f"Executing command: {command}")
    process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    logging.info("Execution completed")

    if process.returncode == 0:
        logging.info(f'Command succeeded: {stdout.decode()}')
    else:
        logging.info(f'Command returned unexpected code: {stderr.decode()}')
