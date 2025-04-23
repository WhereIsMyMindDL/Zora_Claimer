import random
import asyncio

import pandas as pd
from web3 import Web3
from sys import stderr
from loguru import logger
from web3.eth import AsyncEth

delay_wallets = [0, 0]

logger.remove()
logger.add(stderr,
           format="<lm>{time:HH:mm:ss}</lm> | <level>{level}</level> | <blue>{function}:{line}</blue> "
                  "| <lw>{message}</lw>")


class Worker:
    def __init__(self, private_key: str, proxy: str, number_acc: int, cex_address: str = None) -> None:
        self.proxy: str = f"http://{proxy}" if proxy is not None else None
        self.private_key = private_key
        self.cex_address, self.client, self.id = cex_address, None, number_acc
        self.rpc: str = 'https://base.meowrpc.com'
        self.scan: str = 'https://basescan.org/tx/'
        self.w3 = Web3(
            provider=Web3.AsyncHTTPProvider(endpoint_uri=self.rpc),
            modules={"eth": AsyncEth},
            middlewares=[])
        if proxy is not None:
            self.web3 = Web3(
                provider=Web3.AsyncHTTPProvider(endpoint_uri=self.rpc,
                                                request_kwargs={"proxy": self.proxy}),
                modules={"eth": AsyncEth},
                middlewares=[])

        self.account = self.w3.eth.account.from_key(private_key=private_key)

    async def claim(self) -> bool:
        try:
            latest_block = await self.w3.eth.get_block("latest")
            base_fee_per_gas = latest_block["baseFeePerGas"]
            priority_fee = self.w3.to_wei(0.001, 'gwei')
            max_fee_per_gas = base_fee_per_gas + priority_fee

            tx_data = {
                "chainId": 8453,
                "from": self.account.address,
                "to": self.w3.to_checksum_address('0x0000000002ba96C69b95E32CAAB8fc38bAB8B3F8'),
                "nonce": await self.w3.eth.get_transaction_count(self.account.address),
                "value": 0,
                "data": f'0x1e83409a000000000000000000000000{self.cex_address[2:]}',
                "maxFeePerGas": max_fee_per_gas,
                "maxPriorityFeePerGas": priority_fee,
                "gas": await self.w3.eth.estimate_gas({
                    "from": self.account.address,
                    "to": self.w3.to_checksum_address('0x0000000002ba96C69b95E32CAAB8fc38bAB8B3F8'),
                    "value": 0,
                    "data": f'0x1e83409a000000000000000000000000{self.cex_address[2:]}',
                }),
            }

            signed_txn = self.w3.eth.account.sign_transaction(tx_data, self.private_key)
            tx_hash = await self.w3.eth.send_raw_transaction(signed_txn.rawTransaction)
            logger.info(f'#{self.id} | send txs...')
            tx_hash = self.w3.to_hex(tx_hash)
            await asyncio.sleep(6)

            receipt = await self.w3.eth.get_transaction_receipt(tx_hash)
            if receipt['status'] == 1:
                logger.success(f'#{self.id} | Success send tx | hash: {tx_hash}')
                return True

            else:
                logger.error(f'#{self.id} | Failed send tx | hash: {tx_hash}')
                return False

        except Exception as e:
            if '0x646cf558' in str(e):
                logger.info(f'#{self.id} | Already claimed...')
                exel.loc[(self.id - 1), 'Status'] = 'Claimed'
                exel.to_excel('accounts_data.xlsx', header=True, index=False)
            else:
                logger.error(f'#{self.id} | {e}')


async def start(account: list, id_acc: int, semaphore) -> None:
    async with semaphore:
        acc = Worker(private_key=account[0].strip(), proxy=account[1], number_acc=id_acc, cex_address=account[2])
        try:

            await acc.claim()

        except Exception as e:
            logger.error(f'{id_acc} {acc.account.address} Failed: {str(e)}')

        sleep_time = random.randint(delay_wallets[0], delay_wallets[1])
        if sleep_time != 0:
            logger.info(f'Sleep {sleep_time} sec...')
            await asyncio.sleep(sleep_time)


async def main() -> None:
    semaphore: asyncio.Semaphore = asyncio.Semaphore(1)

    tasks: list[asyncio.Task] = [
        asyncio.create_task(coro=start(account=account, id_acc=idx, semaphore=semaphore))
        for idx, account in enumerate(accounts, start=1)
    ]
    await asyncio.gather(*tasks)
    print()


if __name__ == '__main__':
    with open('accounts_data.xlsx', 'rb') as file:
        exel = pd.read_excel(file)
    exel = exel.astype({'Status': 'str'})

    accounts: list[list] = [
        [
            row["Private Key"],
            row["Proxy"] if isinstance(row["Proxy"], str) else None,
            row["Claim Address"] if isinstance(row["Cex Address"], str) else None
        ]
        for index, row in exel.iterrows()
    ]

    logger.info(f'My channel: https://t.me/CryptoMindYep')
    logger.info(f'Total wallets: {len(accounts)}\n')
    asyncio.run(main())

    logger.info('The work completed')
    logger.info('Thx for donat: 0x5AfFeb5fcD283816ab4e926F380F9D0CBBA04d0e')
