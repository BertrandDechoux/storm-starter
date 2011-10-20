package storm.starter.step3.topology;

import java.util.Comparator;
import java.util.List;

public class RankingComparator implements Comparator<List<?>> {
	
	@Override
	public int compare(final List<?> o1, final List<?> o2) {
		return ((Long)o2.get(1)).compareTo((Long)o1.get(1));
	}
	
}