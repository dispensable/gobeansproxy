package cassandra

import (
	"testing"

	"github.com/douban/gobeansproxy/config"
)

var (
	cstarCfgTest = &config.CassandraStoreCfg{
		TableToKeyPrefix: map[string][]string{
			"alg": []string{
				"/zoeva/active_status_user_scores",
				"/yrch/movie/compose_tags_item",
				"/user_profile/note_author/productivity_day_1",
			},

			"misc": []string{
				"/cele_most_expect_mv",
				"/frodo_lander/note",
				"/fuzzy_search/dics/query_correct/book",
			},
			"ark": []string{
				"/ark/alg/works_sims/claim",
			},
			"anti": []string{
				"/anti/book_rating_nums",
			},
		},
		DefaultTable: "misc",
	}
)

func TestKeyTableFinder(t *testing.T) {
	tree, err := NewKeyTableFinder(cstarCfgTest)
	if err != nil {
		t.Fatalf("init keytable finder err %s", err)
	}

	if tree.GetTableByKey("/anti/book_rating_nums/123") != "anti" {
		t.Fatalf("anti table find err")
	}

	if tree.GetTableByKey("/ark/alg/works_sims/claim/fsdlkfjwlekfjwle/fslkdjflkwe") != "ark" {
		t.Fatalf("ark table find err")
	}

	if tree.GetTableByKey("/cele_most_expect_mv/fk/defcc") != "misc" {
		t.Fatalf("misc table find err")
	}

	if tree.GetTableByKey("/user_profile/note_author/productivity_day_1/abc") != "alg" {
		t.Fatalf("alg table find err")
	}

	if tree.GetTableByKey("/send_me_to_misc_table") != "misc" {
		t.Fatalf("default table find err")
	}
}

func BenchmarkKeyTableFinder(b *testing.B) {
	f, err := NewKeyTableFinder(cstarCfgTest)
	if err != nil {
		b.Failed()
	}

	for n := 0; n < b.N; n++ {
		f.GetTableByKey("send_me_to_misc_table")
	}
}
