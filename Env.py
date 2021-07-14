class Env:

    class Db:

        @staticmethod
        def Driver() -> str:
            return "{SQL Server}"
        
        @staticmethod
        def Server() -> str:
            return "DESKTOP-BTN3TLD\TESQL"

        @staticmethod
        def Name() -> str:
            return "CoinData"

        @staticmethod
        def ConnectionString(self) -> str:
            return f"DRIVER={self.Driver()};SERVER={self.Server()};DATABASE={self.Name()};Trusted_Connection=yes;"
