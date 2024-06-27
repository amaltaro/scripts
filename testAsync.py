import asyncio
import sys


class TestAsync():

    def __init__(self, num):
        self.num = num

    async def printNum(self):
        print(f"Num is {self.num}")
        await self.aprintNum()

    async def aprintNum(self):
        print(f"Async num is {self.num}")
        await asyncio.sleep(1)
        print(f"After delay")


if __name__ == "__main__":
    testA = TestAsync(3)
    testA.printNum()
#    testA.aprintNum()