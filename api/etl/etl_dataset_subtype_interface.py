from abc import ABC, abstractmethod

class ETL_Dataset_Subtype_Interface(ABC):

    @abstractmethod
    def set_optional_parameters(self, params):
        pass

    @abstractmethod
    def execute__Step__Pre_ETL_Custom(self):
        pass

    @abstractmethod
    def execute__Step__Download(self):
        pass

    @abstractmethod
    def execute__Step__Extract(self):
        pass

    @abstractmethod
    def execute__Step__Transform(self):
        pass

    @abstractmethod
    def execute__Step__Load(self):
        pass

    @abstractmethod
    def execute__Step__Post_ETL_Custom(self):
        pass

    @abstractmethod
    def execute__Step__Clean_Up(self):
        pass
