package be.nvdh.ts.fetcher;

import static org.joda.time.Duration.ZERO;
import static org.joda.time.LocalTime.MIDNIGHT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.jsoup.Connection.Method;
import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import be.nvdh.ts.domain.FetchResult;
import be.nvdh.ts.domain.Prestation;
import be.nvdh.ts.exception.FetchException;
import be.nvdh.ts.fetch.Fetcher;

public class ElineFetcher implements Fetcher{
	
	private static final String FETCHER_SHORT_NAME 		= "eline";
	private static final String FETCHER_CONFIG_URL		= "url";
	private static final String FETCHER_MINUTES_PER_DAY = "minutesPerDay";
	private static final String FETCHER_MINUTES_LUNCH	= "minutesLunch";
	
	private static final String EVENTARGUMENT    = "__EVENTARGUMENT";
	private static final String EVENTTARGET      = "__EVENTTARGET";
	private static final String EVENTVALIDATION  = "__EVENTVALIDATION";
	private static final String VIEWSTATE        = "__VIEWSTATE";
	
	private static final String COOKIE_AUTHDEF   = ".AUTHDEF";
	private static final String COOKIE_SESSIONID = "ASP.NET_SessionId";

	private static final String REGISTRATIES_CAL_THIS_MONTH = "registraties$CalThisMonth";
	
	private static final String REGISTRATIES_TOTAAL_GEPRESTEERD = "#registraties_lblTotaalGepresteerd";
	private static final String REGISTRATIES_TICK 				= "#registraties_dgrRegistraties__ctl%d_lblRegistratieTijd";
	private static final String REGISTRATIES_MANUEEL			= "#registraties_dgrRgAanvragen__ctl%d_lblRgaTijdstip";
	private static final String REGISTRATIES_DAYCODE 			= "#registraties_dgrPlanningen__ctl2_lblCode";
	private static final String REGISTRATIES_IRREGULARTIES 		= "#registraties_dgrOnregelmatigheden__ctl2_EmLabel2";
	
	private static final int TIMEOUT = 10000;
	private static final int MAX_REGISTRATIONS = 10;

	private static final LocalTime MIDDAY_BOUNDARY = new LocalTime(14, 00);
	
	LocalTime startLunchTime = new LocalTime(12, 0, 0);
	LocalTime endLunchTime = new LocalTime(14, 0, 0);
	
	private int minutesPerDay = 8*60;
	private int minutesLunch = 30;
	
	private String baseUrl = "http://prdecerto/default.aspx";
	private Pattern pagePattern = Pattern.compile("javascript:__doPostBack\\('(.*)','(.*)'\\)");
	private Locale localeBE = new Locale("nl", "BE");
	private DateTimeFormatter dateTimeFormat  = DateTimeFormat.forPattern("dd MMMM").withLocale(localeBE);
	private DateTimeFormatter registrionDateTimeFormat  = new DateTimeFormatterBuilder().appendHourOfDay(2).appendLiteral(":").appendMinuteOfHour(2).toFormatter();
	private PeriodFormatter periodFormat = new PeriodFormatterBuilder().appendHours().appendSuffix("u").appendMinutes().toFormatter();
	
	private String sessionIdCookie;
	private String authdefCookie;

	public FetchResult fetch(Date dateToFetch) throws FetchException {
		try {
			Document homepage = fetchHomePage();
			Document registrationsPage = fetchRegistrationsPage(homepage);
			return fetchData(registrationsPage, new LocalDate(dateToFetch));
		} catch (IOException e) {
			throw new FetchException(e);
		}
	}
	
	public void init(Map<String, String> config) {
		baseUrl = config.get(FETCHER_CONFIG_URL);
		minutesPerDay = Integer.parseInt(config.get(FETCHER_MINUTES_PER_DAY));
		minutesLunch = Integer.parseInt(config.get(FETCHER_MINUTES_LUNCH));
	}
	
	private FetchResult fetchData(Document registrationsPage, LocalDate dateToFetch) throws IOException {

		List<Prestation> prestations = new ArrayList<Prestation>();

		for (LocalDate date : getDaysInMonth(dateToFetch)) {
			Document datePage = fetchDatePage(registrationsPage, date);
			List<LocalTime> tickTimes = getTickTimes(datePage);
			List<LocalTime> manualRegistrations = getManualRegistrations(datePage);
			String irregularities = getIrregularities(datePage);
			String rawDayCode = getRawDayCode(datePage);
			Duration totalDuration = getTotalDuration(datePage, date, tickTimes, manualRegistrations);
			Duration neededDuration = getNeededDuration(date, rawDayCode, totalDuration, tickTimes, manualRegistrations);
			Duration overtime = getOvertime(totalDuration, neededDuration);
			String comment = getComment(irregularities, rawDayCode);
			prestations.add(new Prestation(date, totalDuration, neededDuration, overtime, null, tickTimes, manualRegistrations, rawDayCode, irregularities, comment));
		}
		
		prestations = filterWeekDays(prestations);
		setWeekResults(prestations);
		
		Duration totalTime = getTotalTime(prestations);
		Duration totalOvertime = getTotalOverTime(prestations);
		LocalTime timeToGoHome = getTimeToGoHome(prestations, totalOvertime);
		
		return new FetchResult(new LocalDate(), dateToFetch, prestations, totalTime, totalOvertime, timeToGoHome);
	}

	

	private Duration getNeededDuration(LocalDate date, String rawDayCode, Duration totalDuration, List<LocalTime> tickTimes, List<LocalTime> manualRegistrations) {
		if (isHoliday(rawDayCode)){
			return ZERO;
		}
		
		if (registrationFound(tickTimes, manualRegistrations)){
			if (isToday(date)){
				if (totalDuration.isLongerThan(standardDayDuration())){
					return standardDayDuration();
				} else return totalDuration;
			} else {
				return standardDayDuration();
			}
		}
		
		return ZERO;
	}

	private Document fetchRegistrationsPage(Document previousDocument) throws IOException {
		return Jsoup.connect(baseUrl)
				  .data(requestData(previousDocument))
				  .data(EVENTTARGET, "mnuHoofd")
				  .data(EVENTARGUMENT, "registraties")
				  .userAgent("Mozilla")
				  .cookie(COOKIE_SESSIONID, sessionIdCookie)
				  .cookie(COOKIE_AUTHDEF, authdefCookie)
				  .timeout(TIMEOUT)
				  .post();
	}

	private Document fetchDatePage(Document previousPage, LocalDate date) throws IOException {
		
		String dateAsString = formatDateAsString(date);
		
		Elements link = previousPage.getElementsByAttributeValue("title", dateAsString);
		String linkHref = link.attr("href");
		String dateAsCode = getDateAsCode(linkHref);
		
		return Jsoup.connect(baseUrl)
				  .data(requestData(previousPage))
				  .data(EVENTTARGET, REGISTRATIES_CAL_THIS_MONTH)
				  .data(EVENTARGUMENT, dateAsCode)
				  .userAgent("Mozilla")
				  .cookie(COOKIE_SESSIONID, sessionIdCookie)
				  .cookie(COOKIE_AUTHDEF, authdefCookie)
				  .timeout(TIMEOUT)
				  .post();
	}

	private String formatDateAsString(LocalDate date) {
		return dateTimeFormat.print(date);
	}

	private String getDateAsCode(String linkHref) {
		Matcher matcher = pagePattern.matcher(linkHref);
		if (matcher.matches()){
			return matcher.group(2);
		}
		else return "5510";
	}
	
	private Document fetchHomePage() throws IOException {
		Response response = Jsoup.connect(baseUrl)
					.userAgent("Mozilla")
		 			.method(Method.GET)
		 			.execute();
		fetchCookies(response);
		return response.parse();
	}

	private HashMap<String, String> requestData(Document document) {
		HashMap<String, String> requestData = new HashMap<String, String>();
		requestData.put(VIEWSTATE, scrapeViewState(document));
		requestData.put(EVENTVALIDATION,  scrapeEventValidation(document));
		return requestData;
	}
	
	private String scrapeEventValidation(Document document) {
		return scrapeValueFromElement(EVENTVALIDATION, document);
	}

	private String scrapeViewState(Document document) {
		return scrapeValueFromElement(VIEWSTATE, document);
	}
	
	private void fetchCookies(Response homepageResponse) {
		sessionIdCookie = homepageResponse.cookie(COOKIE_SESSIONID);
		authdefCookie   = homepageResponse.cookie(COOKIE_AUTHDEF);
	}

	private String scrapeValueFromElement(String name, Document document) {
		return document.select("#" + name).first().attr("value");
	}
	
	private List<LocalDate> getDaysInMonth(LocalDate workingDate) {
		List<LocalDate> dates = new ArrayList<LocalDate>();
		int daysOfMonth = workingDate.toDateTimeAtCurrentTime().dayOfMonth().getMaximumValue();
		for (int day = 1; day <= daysOfMonth; day++) {
			dates.add(new LocalDate(workingDate.getYear(), workingDate.getMonthOfYear(), day));
		}
		return dates;
	}
	
	private Duration getTotalDuration(Document datePage, LocalDate date, List<LocalTime> tickTimes, List<LocalTime> manualRegistrations) {
		String prestationAsString = datePage.select(REGISTRATIES_TOTAAL_GEPRESTEERD).first().text();
		Duration totalDuration = periodFormat.parsePeriod(prestationAsString).toStandardDuration();
		
		if (isToday(date) && totalDuration.equals(Duration.ZERO)){
			return figureoutTodaysWorkingHours(datePage, tickTimes, manualRegistrations);
		}
		
		if ((totalDuration == null || totalDuration.isEqual(Duration.ZERO)) && !isEmpty(manualRegistrations)){
			return calculateTotalDurationWithManualRegistrations(totalDuration, date, tickTimes, manualRegistrations);
		}
		
		return totalDuration;
	}
	
	private Duration getOvertime(Duration totalDuration, Duration neededDuration) {
		return totalDuration.minus(neededDuration);
	}
	
	private void setWeekResults(List<Prestation> prestations) {
		for (Prestation prestation : prestations) {
			if (isLastPrestationOfWeek(prestation, prestations)){
				prestation.setWeekOvertime(weekOvertime(prestation, prestations));
				prestation.setLastDayOfWeek(true);
			}
		}
	}
	
	private boolean isLastPrestationOfWeek(Prestation prestation, List<Prestation> prestations) {
		for (Prestation otherPrestation : prestations) {
			if (areInSameWeek(prestation, otherPrestation) && otherPrestation.getDay().isAfter(prestation.getDay())){
				return false;
			}
		}
		return true;
	}
	
	private boolean areInSameWeek(Prestation prestation, Prestation otherPrestation) {
		return prestation.getDay().getWeekOfWeekyear() == otherPrestation.getDay().getWeekOfWeekyear();
	}
	
	private Duration weekOvertime(Prestation prestation, List<Prestation> prestations) {
		Duration weekOvertime = Duration.ZERO;
		for (Prestation otherPrestation : prestations) {
			if (areInSameWeek(prestation, otherPrestation)){
				weekOvertime = weekOvertime.plus(otherPrestation.getOvertime());
			}
		}
		return weekOvertime;
	}
	
	private String getIrregularities(Document datePage) {
		Elements onregelmatigheden = datePage.select(REGISTRATIES_IRREGULARTIES);
		if (!onregelmatigheden.isEmpty()){
			return onregelmatigheden.first().text();
		}
		else return "";
	}
	
	private String getComment(String irregularities, String rawDayCode) {
		if (hasIrregularities(irregularities)){
			return irregularities;
		} else if (isHoliday(rawDayCode)){
			return "Holiday";
		} else if (hasRawDayCode(rawDayCode)){
			return rawDayCode;
		}
		else return "";
	}

	private List<LocalTime> getManualRegistrations(Document datePage){
		return fetchRegistrations(datePage, REGISTRATIES_MANUEEL);
	}
	
	private List<LocalTime> getTickTimes(Document datePage) {
		return fetchRegistrations(datePage, REGISTRATIES_TICK);
	}

	private List<LocalTime> fetchRegistrations(Document datePage, String registrationPattern) {
		List<LocalTime> registrations = new ArrayList<LocalTime>();
		boolean lastFound = false;
		for (int i = 2; i < MAX_REGISTRATIONS && !lastFound; i++){
			String id = String.format(registrationPattern, i);
			LocalTime tickTime = fetchRegistration(datePage, id);
			if (!MIDNIGHT.equals(tickTime)){
				registrations.add(tickTime);
			} else {
				lastFound = true;
			}
		}
		return registrations;
	}
	
	private String getRawDayCode(Document datePage){
		Elements dayCode = datePage.select(REGISTRATIES_DAYCODE);
		if (!dayCode.isEmpty()){
			return dayCode.first().text();
		}
		return DayCode.UNKNOWN.getRawCode();
	}
	
	private Duration getTotalTime(List<Prestation> prestations) {
		Duration totalDuration = Duration.ZERO;
		for (Prestation prestation : prestations) {
			totalDuration = totalDuration.plus(prestation.getDuration());
		}
		return totalDuration;
	}

	private Duration getTotalOverTime(List<Prestation> prestations) {
		Duration totalOvertime = Duration.ZERO;
		for (Prestation prestation : prestations) {
			totalOvertime = totalOvertime.plus(prestation.getOvertime());
		}
		return totalOvertime;
	}
	
	private LocalTime getTimeToGoHome(List<Prestation> prestations, Duration totalOvertime) {
		Prestation todaysPrestation = getPrestationOfToday(prestations);
		if (todaysPrestation != null && todaysPrestation.getTickTimes().size() >0){
			LocalTime firstCheckinTime = firstRegistration(todaysPrestation.getTickTimes(), todaysPrestation.getManualRegistrations());
			LocalTime checkoutTime = firstCheckinTime.plusMinutes(minutesPerDay).plusMillis((int)middayBreak().getMillis()).minusMillis((int)totalOvertime.getMillis());
			if (checkoutTime.isBefore(new LocalTime(14, 30))){
				checkoutTime.minusMinutes(30);
			}
			return checkoutTime;
		}
		return null;
	}
	
	private List<Prestation> filterWeekDays(List<Prestation> prestations) {
		List<Prestation> filteredList = new ArrayList<Prestation>();
		for (Prestation prestation : prestations) {
			if (isWeekDay(prestation.getDay()) || (isWeekendDay(prestation.getDay()) && hasRegistrations(prestation))){
				filteredList.add(prestation);
			}
		}
		return filteredList;
	}
	
	private Prestation getPrestationOfToday(List<Prestation> prestations) {
		for (Prestation prestation : prestations) {
			if (isToday(prestation)){
				return prestation;
			}
		}
		return null;
	}
	
	private LocalTime fetchRegistration(Document datePage, String id) {
		Elements registrations  = datePage.select(id);
		if (registrations != null && !registrations.isEmpty()){			
			String prestationCheckinAsString = registrations.first().text();
			return LocalTime.parse(prestationCheckinAsString, registrionDateTimeFormat);
		}
		return LocalTime.MIDNIGHT;
	}
	
	private Duration calculateTotalDurationWithManualRegistrations(Duration totalDuration, LocalDate date, List<LocalTime> tickTimes, List<LocalTime> manualRegistrations) {
		Duration fallback = totalDuration;
		List<LocalTime> allRegistrations = getAllRegistrations(tickTimes, manualRegistrations);
		
		if (allRegistrations.size() %2 == 0){
			Duration totalCalculatedTime = calculateDurationOverIntervals(allRegistrations);
			totalCalculatedTime = compensateForLunch(totalCalculatedTime, allRegistrations);
			if (totalDuration.isLongerThan(totalCalculatedTime)){
				return fallback;
			} else {
				return totalCalculatedTime;
			}
		} 
		
		return fallback;
	}

	private List<LocalTime> getAllRegistrations(List<LocalTime> tickTimes, List<LocalTime> manualRegistrations) {
		List<LocalTime> allRegistrations = new ArrayList<LocalTime>();
		allRegistrations.addAll(tickTimes);
		allRegistrations.addAll(manualRegistrations);
		sort(allRegistrations);
		allRegistrations = removeDuplicates(allRegistrations);
		return allRegistrations;
	}

	private void sort(List<LocalTime> allRegistrations) {
		Collections.sort(allRegistrations);
	}

	private Duration compensateForLunch(Duration totalCalculatedTime, List<LocalTime> allRegistrations) {
		if (!needsCorrectionForLunch(allRegistrations)){
			totalCalculatedTime = totalCalculatedTime.minus(middayBreak());
		}
		return totalCalculatedTime;
	}

	private Duration middayBreak() {
		return new Duration(minutesLunch*60*1000);
	}

	private boolean needsCorrectionForLunch(List<LocalTime> allRegistrations) {
		Duration timeDuringLunchHours = calculateTimeOutDuringLunchHours(allRegistrations);
		if (timeDuringLunchHours.isLongerThan(middayBreak())){
			return true;
		}
		return false;
	}

	private Duration calculateDurationOverIntervals(List<LocalTime> allRegistrations) {
		Duration totalDuration = Duration.ZERO;
		boolean started = false;
		LocalTime startTime = null;
		for (LocalTime localTime : allRegistrations) {
			if (!started){
				startTime = localTime;
				started = true;
			} else {
				totalDuration = totalDuration.plus(durationBetween(startTime, localTime));
				started = false;
			}
		}
		return totalDuration;
	}
	
	private Duration calculateTimeOutDuringLunchHours(List<LocalTime> allRegistrations) {
		boolean started = false;
		boolean lunchTime = false;
		LocalTime startTime = null;
		for (LocalTime localTime : allRegistrations) {
			if (!started){
				startTime = localTime;
				if (isInLunchHours(localTime)){
					lunchTime = true;
				}
				started = true;
			} else {
				if (lunchTime && isInLunchHours(localTime)){
					return durationBetween(startTime, localTime);
				}
				else if (!lunchTime && isInLunchHours(localTime)){
					return durationBetween(startLunchTime, localTime);
				} else if (lunchTime && !isInLunchHours(localTime)){
					return durationBetween(startTime, endLunchTime);
				}
				started = false;
			}
		}
		return Duration.ZERO;
	}

	private boolean isInLunchHours(LocalTime localTime) {

		return localTime.isAfter(startLunchTime) && localTime.isBefore(endLunchTime);
	}

	private List<LocalTime> removeDuplicates(List<LocalTime> allRegistrations) {
		List<LocalTime> filteredList = new ArrayList<LocalTime>();
		for (LocalTime localTime : allRegistrations) {
			if (!allReadyInList(filteredList, localTime)){
				filteredList.add(localTime);
			}
		}
		return filteredList;
	}

	private boolean allReadyInList(List<LocalTime> filteredList, LocalTime localTime) {
		for (LocalTime localTimeInList : filteredList) {
			if (localTime.isEqual(localTimeInList)){
				return true;
			}
		}
		return false;
	}

	private Duration figureoutTodaysWorkingHours(Document datePage, List<LocalTime> tickTimes, List<LocalTime> manualRegistrations) {
		if (registrationFound(tickTimes, manualRegistrations)){
			LocalTime checkinTime = firstRegistration(tickTimes, manualRegistrations);
			Duration durationUntilNowSinceCheckin = durationUntilNowSince(checkinTime);
			if (new LocalTime().isAfter(MIDDAY_BOUNDARY)){
				return durationUntilNowSinceCheckin.minus(middayBreak());
			}
			return durationUntilNowSinceCheckin;
		} else return Duration.ZERO;
	}

	private LocalTime firstRegistration(List<LocalTime> tickTimes, List<LocalTime> manualRegistrations) {
		return !isEmpty(tickTimes) ? tickTimes.get(0) : manualRegistrations.get(0);
	}
	
	private boolean hasRegistrations(Prestation prestation){
		return registrationFound(prestation.getTickTimes(), prestation.getManualRegistrations());
	}

	private boolean registrationFound(List<LocalTime> tickTimes, List<LocalTime> manualRegistrations) {
		return !isEmpty(tickTimes) || !isEmpty(manualRegistrations);
	}

	private Duration durationUntilNowSince(LocalTime registrationDateTime) {
		LocalTime start = registrationDateTime;
		LocalTime end = new LocalTime(now().getHourOfDay(), now().getMinuteOfHour(), now().getSecondOfMinute());
		return durationBetween(start, end);
	}
	
	private Duration durationBetween(LocalTime start, LocalTime end){
		return new Interval(start.getMillisOfDay(), end.getMillisOfDay()).toDuration();
	}
	
	private boolean hasIrregularities(String irregularities) {
		return hasRawDayCode(irregularities);
	}

	private boolean hasRawDayCode(String rawDayCode) {
		return !StringUtils.isEmpty(rawDayCode);
	}
	
	private boolean isWeekDay(LocalDate localDate) {
		return !isWeekendDay(localDate);
	}
	
	private boolean isWeekendDay(LocalDate localDate) {
		return localDate.getDayOfWeek() == DateTimeConstants.SUNDAY || localDate.getDayOfWeek() == DateTimeConstants.SATURDAY;
	}
	
	private boolean isHoliday(String rawDayCode) {
		return DayCode.HOLIDAY.getRawCode().equalsIgnoreCase(rawDayCode) || DayCode.CAO.getRawCode().equalsIgnoreCase(rawDayCode);
	}
	
	private Duration standardDayDuration() {
		return new Duration(minutesPerDay * 60 * 1000);
	}
	
	public static boolean isEmpty(Collection<?> coll){
		return (coll == null) || (coll.isEmpty());
	}

	private DateTime now() {
		return new DateTime();
	}
	
	private boolean isToday(Prestation prestation) {
		return isToday(prestation.getDay());
	}
	
	private boolean isToday(LocalDate date) {
		return date.isEqual(new LocalDate());
	}

	public String getName(){
		return FETCHER_SHORT_NAME;
	}
	
	public String toString(){
		return FETCHER_SHORT_NAME;
	}

}
