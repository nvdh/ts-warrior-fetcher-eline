package be.nvdh.ts.fetcher;

public enum DayCode {
	
	NORMAL("L"),
	HOLIDAY("FEE"),
	CAO("CAO"),
	UNKNOWN("");
	
	private String rawCode;
	
	DayCode(String rawCode){
		this.rawCode = rawCode;
	}
	
	public static DayCode fromRawCode(String rawCode){
		for (DayCode dayCode : DayCode.values()) {
			if (dayCode.getRawCode().equals(rawCode))
				return dayCode;
		}
		return DayCode.UNKNOWN;
	}
	
	public String getRawCode() {
		return rawCode;
	};

}
